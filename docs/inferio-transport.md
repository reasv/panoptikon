# Inference transport

How the gateway and the inference server talk to each other over HTTP: the
client's connection model and failure typing, and the server side of the same
wire.

## Client (inferio_client.rs)

The client in `panoptikon/src/inferio_client.rs` drives one or more inference
endpoints. Local inference is loopback HTTP inside the same process, so an
in-flight predict costs two descriptors in one table (the client socket and
the accepted server socket); the gateway and the inference server also run on
separate machines in real deployments, so there is one code path for both.

### Transport selection

HTTP/2 cleartext (h2c) with **prior knowledge**, falling back to HTTP/1.1.
Prior knowledge rather than an h2c upgrade because there is no TLS to carry
ALPN and the upgrade dance costs a round trip per connection.

**Over TLS the probe negotiates.** An `https://` upstream — a TLS front
ahead of a remote inference server — carries ALPN, so its probe goes out on a
negotiating client that offers `h2` and `http/1.1` (the `native-tls-alpn`
feature: without it reqwest advertises no protocol at all) and records
whichever version came back. Assuming h2 in the probe hands the preface to a
front that has chosen HTTP/1.1, and both shapes that produces are dead ends:
the probe fails and the endpoint is memoized `Http11` for the life of the
process, or the front aborts the handshake and the `is_connect` error is
excluded from the memo, so every request re-probes. A failed TLS probe is
therefore never protocol evidence — the same client would have negotiated
HTTP/1.1 had the peer offered it.

The lanes themselves use prior knowledge on both schemes, which over TLS
means offering only `h2` in ALPN, and they are used only once the probe has
seen the peer choose h2. A lane that negotiated per connection costs a socket
per request of a cold burst: hyper-util allows one connect in flight per pool
key only when the client is HTTP/2-only, and a negotiating client cannot know
a connection will be h2 until its handshake ends, so every request that finds
no ready connection dials its own. A 128-request burst through a TLS front
opened 64 connections that way and opens 2 now, as it does in the clear. A
front later restarted without h2 fails the lane's handshake, which is a
connection error, which clears the memo and re-probes.

**The choice is logged.** One INFO line per endpoint, `inference transport
chosen`, with `transport` (`h2c`, `h2` over TLS, `http/1.1`), `previous` and
`reason` (`ALPN selected h2`, the prior-knowledge answer, the refusal
fallback, a provisional timeout). It is written when a probe *records* a
transport that differs from the last one logged, so a re-probe that finds the
same answer, and an unreachable peer (which records nothing), stay silent.

The same `https://` upstream is also dialed by the gateway's
`/api/inference/*` proxy (`proxy.rs`), which is a second client on a second
stack: hyper-util over hyper-tls, cleartext for an `http://` upstream and TLS
for an `https://` one, because it streams bodies and bridges upgrades and so
cannot be expressed on reqwest. Neither client exposes a trust store option,
so a front with a private CA is trusted only through `SSL_CERT_FILE` in the
gateway's environment — today's only mechanism.

The transport is resolved by a one-time probe (`GET /cache`, the cheapest
thing the surface serves) sent with prior knowledge. *Any* answer proves the
peer speaks h2c — a 404 or a 500 is as good as a 200, because reading a status
at all means the frames parsed.

**A downgrade is only ever recorded on positive evidence.** It is recorded
once and only a predict-time connection error clears it, so one wrong memo
costs the endpoint its multiplexing for the process lifetime and halves every
job's in-flight window with it (`requests_are_multiplexed`). First contact in
the split deployment crosses a network, where blips are ordinary.

`reqwest` cannot distinguish "the peer rejected the h2 preface" from "the
connection died mid-stream" — both are `Kind::Request` — so the ambiguous
class is resolved by asking twice more: the h2 probe is repeated (a reset
twice in a row is not a blip), and then the peer must *answer over HTTP/1.1*,
proving it is alive and therefore that its refusal was about the protocol.
Anything short of that records nothing and re-probes next call. A failure that
is `is_connect` or `is_timeout` is a network fact, never a protocol fact, and
is excluded up front; without that check a slow endpoint would permanently
downgrade itself.

A connection error at predict time forgets the memo, because a server can be
restarted into a build speaking the other protocol. A `REFUSED_STREAM` is the
exception and the memo is *kept*: only an h2 peer can refuse a stream, so it
is evidence for the memo, not against it. A connection **closed under a
request** is the second exception, and it is kept until the retry that
follows it fails the same way: behind a proxy that close is a race rather
than an event — `reqwest`'s `pool_idle_timeout` is 90 s against nginx's
default `keepalive_timeout` of 75 — and reading the first one as a protocol
change costs every request in flight a probe of its own.

Non-predict calls funnel their send
result through `checked_send` for the same rule, otherwise a memo can go stale
*upward* — a peer remembered as h2c that reappears behind an HTTP/1.1-only
proxy fails `load_model` on every job forever, and a job that fails at load
never reaches the predict that would have cleared the memo.

**One prober at a time.** A caller that finds no memo takes the probe lock,
and one that waited out somebody else's probe takes that probe's verdict,
including the verdicts deliberately not recorded. Otherwise a single dropped
memo is one three-request probe per request in flight. The probe is the one
request on these clients with a deadline (`PROBE_TIMEOUT`, 5 s): it is taken
under that lock and everything else here has no request timeout, so a peer
that accepts and never answers would hold every caller of the endpoint
behind the prober for as long as it cares to keep the socket. A probe that
runs out of that deadline records HTTP/1.1 **provisionally**
(`PROVISIONAL_MEMO_TTL`, 60 s) rather than nothing: a peer persistently
slower than 5 s on `/cache` would otherwise make every non-coalesced call pay
a fresh probe and never multiplex, while a permanent memo would write off a
peer that was merely slow once.

### Lanes and the stream limit

| Constant | Value | Bounds |
| --- | --- | --- |
| `INFERENCE_CONNECTION_LANES` | 64 | independent h2 connections (sockets) per endpoint |
| `H2_STREAMS_PER_CONNECTION` | 64 | streams offered one lane before the next is recruited |
| `INFERENCE_MAX_CONCURRENT_REQUESTS` | 256 | gate floor on both transports |
| `INFERENCE_MAX_CONCURRENT_STREAMS` | 4096 | gate ceiling (lanes x streams); HTTP/1.1 also within the descriptor budget |

A "lane" is its own `reqwest::Client` with its own pool, which is the only way
to make the number real: for HTTP/2 hyper-util's pool hands every caller the
*same* connection (`Reservation::Shared`, `can_share() == is_http2()`) and its
readiness test is "the dispatch channel is open", not "there is stream
capacity", so a single client never opens a second connection however wide the
window gets. hyper-util also dedups concurrent h2 connects per pool key, so a
burst cannot fan one lane into several sockets — for a prior-knowledge client
only, which is why TLS lanes use it too (see "Transport selection").

**Why 64 lanes.** `lanes x H2_STREAMS_PER_CONNECTION` = 4096 requests reaches
the job's own in-flight ceiling: `jobs::extraction::in_flight_unit_ceiling`
admits 4096 units at the shipped defaults, and for an `item`/`count` model
(every image tagger and CLIP embedder) one unit is one request. Anything less
makes the client a ceiling nothing else in the system knows about.

**Why 64 streams per lane.** A peer's real limit is invisible: `reqwest`
exposes no way to read its `SETTINGS_MAX_CONCURRENT_STREAMS`, and offering
more streams than it allows does not fail — it silently queues inside `h2`,
where neither the dispatcher nor `/health` can see it. 64 is below every
common server default (nginx 128, Envoy 100, hyper 200, this binary's own
`MAX_CONCURRENT_STREAMS` of 512).

**What lanes cost.** Lanes are *recruited by load*, not spread across
(`EndpointRuntime::pick_lane`): the recruited prefix `0..k` is the smallest
that can hold the current load at 64 streams each, and the choice is
least-loaded within it (h2 streams on one connection share a TCP window, so a
lane carrying a slow batch should not also be handed the next one). Spreading
64 concurrent predicts over 64 lanes would cost 64 sockets — HTTP/1.1's price
with extra steps. The descriptor cost is therefore `ceil(in_flight / 64)`
sockets, at most 64; local inference doubles that to 128 against
`jobs::extraction`'s `FD_RESERVE` of 256 and the shipped container's soft
limit of 1024.

Each lane's client is built on first use, because a `reqwest::Client` carries
a connector and a TLS context worth roughly 620-720 KiB of RSS. Registering an
endpoint costs ~1.4 MiB (the eagerly built lane 0 plus the HTTP/1.1 client),
and the 64-lane worst case is ~43 MiB, paid a lane at a time by the work that
needs it. Building all lanes up front cost ~58 MiB per endpoint on first
contact whatever the load. Lane 0 is eager so that "can this process talk to
this endpoint at all" is answered at registration, and so a later lane's build
failure can fall back to it — a request must not fail because a *second*
connection could not be prepared.

**The window is fixed and larger, on both ends.** hyper's defaults (1 MiB
per stream and per connection on this server, 2 MiB and 5 MiB on the client)
are a throughput cap the moment the endpoint is a round trip away: lanes are
recruited by load, so below 64 concurrent predicts every body shares one
connection and one window, and one window per RTT at 40-80 ms is tens of MB/s
whatever the link can carry — where HTTP/1.1 had 256 independent sockets.
Both the client (`h2_client_builder`) and this server (`serve_with_streams`)
therefore name the same two: `H2_STREAM_WINDOW` = 4 MiB and
`H2_CONNECTION_WINDOW` = 16 MiB, hyper's own adaptive ceiling.

Fixed rather than `adaptive_window`, which was measured and reverted.
Adaptive sets *both* windows to the spec's 65 535 and grows them only as its
own pings are acknowledged, so it pays a ramp on every new connection and on
loopback the ramp never earns itself back: curl-measured upload throughput
into this server fell 35-50 % (1 MiB body 25.0 -> 12.4 MB/s, 64 MiB
133.9 -> 80.8, eight concurrent 16 MiB 386 -> 250) while a 20 ms round trip
gained. The fixed windows take the round trip's gain without the loopback
loss: against the pre-change binary, loopback is 1.03-1.63x on every body
size measured, and at 20 ms RTT the 16 MiB body is 1.99x and the 64 MiB body
1.78x (adaptive: 1.38x and 1.96x).

The connection window is the buffering bound, not the stream window times
`MAX_CONCURRENT_STREAMS`: every DATA byte is charged to both windows, so 512
streams at 4 MiB each cannot buffer 2 GiB — one connection holds at most
`H2_CONNECTION_WINDOW` = 16 MiB of unread data however many streams it opens,
and h2 allocates that as frames arrive rather than reserving it. What a
*predict* body may hold is bounded separately, by
`inferio::http::PREDICT_INFLIGHT_BODY_BYTES`.

**Per connection, so multiply.** `serve_with_streams` drives *every* listener
this process binds — the gateway's primary plus each `[[server.endpoints]]`,
and the standalone inferio listener — and nothing bounds how many connections
a peer opens, so the process-wide unread-body bound is connections x 16 MiB,
never 16 MiB. The gateway's public endpoint is one of those listeners and its
proxied routes carry no predict body budget at all: there the connection
window is the whole bound. On the client side the same multiplier is
`INFERENCE_CONNECTION_LANES` = 64 connections x 16 MiB per endpoint, since
each lane is its own pool and therefore its own connection.

### The in-flight gate

Every admitted request holds a semaphore permit; queued requests hold none, so
a queued request costs nothing where an admitted HTTP/1.1 one costs a socket.

Both gates follow the endpoint's desired-in-flight figure
(`DESIRED_IN_FLIGHT_HEADER`, see `docs/inferio-worker-protocol.md`):
`set_in_flight_target` sets each, clamped between
`INFERENCE_MAX_CONCURRENT_REQUESTS` and the gate's ceiling. The floor means
this can only ever *raise* a gate above what every existing deployment
already runs at.

Under **h2c** the ceiling is `INFERENCE_MAX_CONCURRENT_STREAMS`: a published
figure can never make the client offer a lane more streams than it was
designed to, and never moves the descriptor cost at all (bounded by the lane
count, not the gate).

Under **HTTP/1.1** an admitted request *is* a socket, two descriptors with
local inference (both ends are in this process), and queued requests hold
none. The ceiling is therefore also what the descriptor budget holds:
`http1_gate_ceiling` = (soft `RLIMIT_NOFILE` - `FD_RESERVE` 256) / 2, read
when the endpoint is first used and kept between the floor and 4096. At the
shipped container's soft limit of 1024 that is 384; from 8448 up it is 4096,
the same depth as h2c. It is the bound `in_flight_unit_ceiling` puts on a
job's window over HTTP/1.1, so the gate is never the tighter of the two.
Below a soft limit of 768 the floor's 256 sockets exceed the budget, and the
job's window, which may go lower, is the bound. An image model sends one item
per request, so over HTTP/1.1 the gate is the most items the server can hold
for batching; a fixed 256 held it well below the server's own figure. The
gate is taken on both transports because HTTP/1.1 is reachable *after* a job
has sized its window for multiplexing — `in_flight_unit_ceiling` is evaluated
once, before the item loop, so a peer restarted mid-job into a build without
HTTP/2 flips the transport under a window sized for h2c.

A fixed 256 was justified as "four times a job's in-flight budget (4096 units
at 64 units per request)", which only holds for a model whose items carry 64
units each. An image item carries one, so 4096 units is 4096 concurrent
requests, and 256 was 1/16 of the budget rather than 4x it — a throughput cap
for exactly the models the feature exists for.

The published figure is in *items* and the gate counts *requests*. Using it
directly is conservative in the safe direction: for the models that matter one
item is one request, and for a model packing several units per item it
over-provisions a bound whose only cost is permits. The job's
own `UnitBudget` remains the throttle. Several models share one endpoint, so
this is last-writer-wins, which is acceptable exactly because of the floor: the
worst a small model can do to a large one is put the gate back to the constant.

A **shrink never takes a permit away from a request already in flight** — it
withholds permits as they come back (`Gate::release`), the same rule as
`jobs::extraction::UnitBudget`. Dropping the permit would not do: `Semaphore`
hands a released permit straight to a waiter and a saturated job always has
waiters, so `forget_permits` alone can never land a shrink. `/health`
therefore reports in-flight as `target + pending_shrink - available`, because
permits in existence are `target + pending_shrink`; subtracting from `target`
alone reports a saturated, shrinking endpoint as idle, which is the one moment
the number is worth reading.

### Retries

`predict` owns a bounded retry loop (`PREDICT_MAX_RETRIES` = 3, exponential
between `PREDICT_MIN_DELAY` and `PREDICT_MAX_DELAY`). It retries 429/502/503/
504 and, through `should_retry_error`, connect, timeout, `REFUSED_STREAM` and
the three shapes of a connection dying under a request that was already sent:
hyper's `IncompleteMessage` (`is_connection_closed`), hyper's `is_canceled`,
and an `io::Error` of kind `ConnectionReset` or `ConnectionAborted`
(`is_connection_lost`) — what a peer that reads the request and then closes
with `SO_LINGER 0` produces. The last two are `reqwest_retry`'s own transient
classes, and this surface replaces that strategy wholesale, so leaving them
out would mean `load_model` failing on the first reset. `IncompleteMessage`
is an HTTP/1.1-path class — hyper raises it only in `proto/h1` — so of the
three it is the one that applies once the memo is `Http11`. The
lease (gate permit + lane claim) is dropped before every backoff wait and
re-resolved per attempt: a retry that held its permit across the wait would
hold a concurrency slot while doing nothing, precisely when the server has
said it is overloaded, and a connection error between attempts may have
changed the transport.

`REFUSED_STREAM` is reachable in ordinary operation, not only under abuse:
hyper's client opens up to `DEFAULT_INITIAL_MAX_SEND_STREAMS` = 100 streams on
a new connection *before* the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` frame
has been read, and `reqwest` exposes no way to lower that. A burst opened the
instant a lane connects to a peer advertising fewer than 100 has some streams
refused every time until the SETTINGS land. RFC 9113 §8.7 defines it as "not
processed", so it is unambiguously safe to retry. The error chain is walked
for `h2::Reason::REFUSED_STREAM` rather than matched on a string.

A keep-alive timeout and a request failed by the health checks (see "Dead
peers") are not retried in place: the peer has been silent for about 50 s
already, and the job's single re-queue is the retry.

A load-failure cooldown (`LOAD_COOLDOWN_KIND`) is the one 503 that must not be
retried: the server is naming when to come back, and a caller that keeps
asking burns the whole cooldown window one request at a time.

The other endpoints have no loop of their own and run on the retry
middleware, whose default calls every 5xx transient. It is narrowed to
429/502/504 plus `should_retry_error`'s classes — the same ones `predict`
retries, and the same transient errors the stock strategy would have
retried — because from there the body is
unread and neither final answer this surface gives can be recognised: a 503
is the cooldown, and a 500 from `PUT /load` is a failed load — including one
that just spent the worker's 600 s load deadline, where three more attempts
are three more worker spawns with that deadline each.

### Failure kinds

`InferenceFailure` is a typed error attached to the returned `anyhow::Error`,
so callers reach it with `downcast_ref` and the decision survives the error
being wrapped in context on the way up. `status` is 0 when there was no
response at all. `kind` is `None` for any failure that answered with a plain
string detail (an older server, an unrelated 4xx/5xx).

| `detail.kind` | Status | Origin | Meaning |
| --- | --- | --- | --- |
| `worker_died` | 5xx | server | the worker process died with the request in flight |
| `request_incomplete` | 400 | server | the request body never arrived in full, so nothing was parsed |
| `body_budget_exhausted` | 503 + `Retry-After` | server | the server had no room to read the body; clears as bodies ahead finish |
| `request_too_large` | 413 | server | the body was over the per-request limit and was refused unread |
| `load_cooldown` | 503 | server | the model is inside its per-model load-failure cooldown |
| `transport` | 0 | **this client** | the predict ended before an answer was read, or read to its end |

`request_incomplete` is the one 400 that must not be read as a verdict: the
status is right about the request and says nothing about the items.

`transport` never travels on the wire and cannot — no server can report that
its own answer failed to arrive. `InferenceFailure::parse` therefore leaves
the `transport` field `None` whatever the body says, so a peer answering
`{"kind": "transport"}` buys nothing with it; only `from_transport`, called
with a `reqwest` error this process held, writes it. `last_error` carries the
**whole source chain**, which is the half worth having: `reqwest`'s own
`Display` names the layer ("error sending request for url (…)") while the
cause underneath is `h2` saying `REFUSED_STREAM` or `GOAWAY`, or `hyper`
saying the connection closed before the message completed.

### Transport phases

`TransportPhase` records how far a predict got, which is the whole of what a
transport failure says about the item. The variants are in request order and
the load-bearing boundary is between `Headers` and `Body`.

| Phase | What happened | Answer existed? |
| --- | --- | --- |
| `Connect` | no connection: refused, unreachable, DNS/TLS, connect timeout | no — nothing left this process |
| `Send` | connection up, no response head: reset, `REFUSED_STREAM`, body not writable | no |
| `Headers` | request delivered, no response head inside the deadline | no |
| `Body` | head arrived, body lost: `GOAWAY` mid-body, reset, truncation, read timeout | yes, and this end lost it |

`send()` resolves when the response head arrives, so every error it can report
is at or above `Headers`; `send_phase` orders `is_connect` before `is_timeout`
because `reqwest`'s predicates are not disjoint and "nothing left this
process" is the stronger claim about a connect timeout.

`Send` does not claim the batch was never parsed — `reqwest` reports the same
`Kind::Request` for a request that never landed and for a connection that died
with a whole request on it. It claims only what is observable: no answer had
been produced. One case slips in from below: a server whose response body
fails immediately resets the stream, and a reset that overtakes its own
response head is observed as `Send` rather than `Body`. That over-claims in
the harmless direction — both buy the same single re-queue.

`request_too_large` is the one unparsed refusal that is **not** in
`is_unattempted()`. Nothing was attempted, but it is deterministic: the set
buys a re-submission, and the same bytes get the same answer. The recovery is
a smaller request, and it belongs to the sender — `run_chunked_inference`
halves the chunk and sends both halves, and only an input still refused alone
is the item's own failure. The split is keyed on the status, so an
**untyped** 413 — a reverse proxy's own body limit, with no `detail.kind`,
such as nginx's `client_max_body_size` (1 MiB by default) — splits the same
way. An input refused alone is recorded `resource`, as the typed case is: a
limit of this deployment rather than of the media, so it is not re-sent every
run, and the reason names the input's size and, for an untyped 413, the proxy
setting to raise. Like every `resource` row it is cleared by a retry
directive, not by the limit being raised.

`is_unattempted()` is true for the three server kinds above plus every
transport phase before `Body`. The standard is *no verdict was produced*,
not *no work was done*: `Send` and `Headers` may leave a server mid-inference
whose result nobody will read, but that residue is a wasted GPU pass, not a
verdict, and recording the item as failed would be a claim about the media
made on no evidence. It is keyed on the typed kind and never on the status —
an untyped 4xx is not evidence of anything, since a stock FastAPI upstream
answers 400 for a genuinely bad request too.

`warrants_resubmission()` is what a job's re-queue policy actually asks, and
adds `Body` to that set: a predict is a pure, idempotent inference over the
inputs in its body, writing nothing outside its response (the model cache is
keyed and would simply be hit again), so a lost answer leaves the item exactly
as undone as a lost request and asking again can only cost a repeated GPU
pass.

### Request authority (policy.rs)

`policy::request_authority` is the single definition of "the host this request
is for": the request target's authority when the URI carries one, otherwise the
`Host` header, verbatim — no case folding, no port stripping, and any
deprecated `userinfo@` prefix left in place so a caller that must refuse one
still sees it. `None` means neither source named an authority, and every caller
reads that as unknown rather than as a match.

The order is what both HTTP versions say the authority *is*. An HTTP/2 request
carries its authority in `:authority` and normally sends no `Host` header at
all (RFC 9113 §8.3.1); hyper puts that on the request URI, so `Uri::authority`
returns it. The same field carries an HTTP/1.1 absolute-form request target,
which RFC 9112 §3.2.2 likewise makes override `Host`. Reading `Host` alone
would leave an h2c request hostless, and the same request must select the same
policy over both transports.

Reading the authority introduces no new trust: `:authority` is exactly as
client-controlled as `Host`, and any client that can set one can set the other.
The precedence only decides which of two client-chosen names picks a policy in
the malformed case where both are present and disagree (RFC 9113 §8.3.1
requires them to be consistent). Non-spoofable routing remains the listener
endpoint (`ListenerEndpoint`).

`resolve_effective_host` normalizes that authority for `[policies.match]
hosts` comparison (userinfo, port and IPv6 brackets removed, lowercased) and
layers the trusted forwarded headers on top: `Forwarded` /`X-Forwarded-Host`
win, but only when `[server] trust_forwarded_headers` is set — the
reverse-proxy deployment, where the front proxy rather than the request's own
framing is the authority on the name the client used. A request with neither an
authority nor a `Host` stays hostless, and `select_policy` then matches only
policies that state no `hosts`.

Both consumers go through the same function, so the same request cannot be
judged by two different names: the policy layer selects `[policies.match]
hosts` with it, and the Desktop bridge guard (`api::desktop`) checks browser
same-origin with it.

### Dead peers

No request here has a deadline on its work: a batch may legitimately take
minutes. A peer that stops answering is found by the connection, or by a
health check when the connection cannot see it.

- **HTTP/2** lanes send a PING after `H2_KEEP_ALIVE_INTERVAL` (30 s) without
  a frame from the peer while a request is open, and close the connection when
  one goes unanswered for `H2_KEEP_ALIVE_TIMEOUT` (20 s). A working peer
  answers from its connection task while it infers, so this bounds silence,
  not work. Every request on the connection then fails with a keep-alive
  timeout: phase `Headers` (or `Body`), re-queued once by the job, not retried
  in place, and not evidence against the memo (`invalidates_transport_memo`
  excludes timeouts).
- Behind a TLS reverse proxy the pings end at the proxy, which answers them
  itself, so they cannot see a frozen backend.
- A ping queues behind whatever the connection is already sending. During a
  large upload on a slow uplink, a send buffer of several MB below about
  1.5 Mbit/s can hold it long enough to approach the 20 s timeout.
- **HTTP/1.1** has no ping. reqwest's defaults set TCP keep-alive (15 s idle,
  then 3 probes 15 s apart) and, on Linux, `TCP_USER_TIMEOUT` of 30 s, so a
  dead host or a broken path fails the socket in about a minute. A peer whose
  *process* froze keeps its kernel acknowledging, so the socket never fails.
- **Health checks** cover both gaps. Once a request to a base URL has had no
  response head for 30 s (`HEALTH_CHECKS`), that endpoint sends
  `GET /api/inference/health` through the same base URL, so through the same
  proxy, on a new connection of its own (a lane could queue it behind the
  predicts) that picks the version as the requests do: ALPN over TLS, and in
  the clear h2 with prior knowledge or HTTP/1.1 as the transport in force
  says. It has a 10 s deadline and runs again every 10 s while any request
  still waits, and while the last check missed short of a verdict: a miss is
  kept when the keep-alive fails the waiting requests first, so their
  re-submissions are cut off at the verdict instead of starting a new stall.
  One task per base URL runs them. Besides a waiting request,
  only a request to a server declared frozen starts one (see "Health"). A
  check misses on its deadline or on a 502, 503 or 504, a proxy saying the
  server behind it did not answer. Any other outcome, a refused connection or
  a failed TLS handshake included, is no evidence of a freeze, and neither is
  a missed check when a response other than those came from the base URL
  since the previous check. `/health` reads in-memory state and touches no
  model, so a busy server answers it and a long batch is never cut off.
- `HEALTH_CHECK_MISSES` (2) checks in a row without an answer declare the
  server frozen, about 50 s into the stall, and log one WARN. Every request
  waiting on it fails as a keep-alive timeout fails it (phase `Headers`, class
  `timeout`, re-queued once by the job). Until a check answers again (one
  INFO), new requests fail without being sent, except one at a time that
  starts a check and waits for its verdict. A job running when the server
  froze ends as soon as it has prepared its items: `partial`, or `failed` if
  no item had succeeded, with the items owed either way. A search fails with
  504 `Could not reach the inference server at …: it did not answer 2 health
  checks in a row`.
- A check must reach the server on a new connection. A proxy that caps its
  connections to the server (HAProxy `maxconn`, nginx `max_conns`) can queue
  the check behind predicts until it times out. A response arriving meanwhile
  answers the check, so a busy server is declared frozen only when no request
  is answered during two checks in a row; the README says how to avoid it.
- A predict with no response head also logs a WARN after `STALL_WARN_AFTER`
  (120 s) and again each time the wait doubles (240 s, 480 s, …).

### Repeated log lines

A failure that repeats once per request — an unreachable probe, a predict
failure, a re-queue, a transient item failure, a refused predict on the
server, a 5xx in the HTTP trace — goes through `log_throttle::LogThrottle`,
keyed by what distinguishes the line (model and failure family for predicts,
the error text for item failures, the status for the trace) so that a
distinct error is never hidden behind another. Per key, the first occurrence
logs in full, the rest in the next `LOG_REPEAT_WINDOW` (10 s) are counted,
and one line reports the count when the window closes: from a timer, or from
the next occurrence if no timer ran.

### Health

`InferenceTransportHealth` reports, per endpoint, the transport in force, the
lanes available and the lanes actually carrying a request, the gate's current
target and what is in flight. Every field is a measured quantity rather than a
constant restated, and it is read off the shared endpoint registry so it
covers the job pool, the PQL path and the preload loop alike (they are the
same runtime per base URL). A node that only serves inference reports none.
The registry mutex is taken normally — it is held for the few instructions of
a lookup, never across an await — but each endpoint's *transport* is read with
`try_read`, so a health probe never waits on an in-flight transport probe and
an endpoint being resolved reports `unknown`. `try_lock` on the registry is
wrong: under any concurrent client construction it reports an empty client
section.

With inference remote (`inference_local.enabled = false`) the gateway's
`GET /api/inference/health` is the upstream's report with `inference_clients`
replaced by the gateway's own, since the clients that dial the upstream live
in the gateway. Anything but a 200 JSON object passes through untouched, so
while the upstream is down the gateway's section is visible only in its log,
and a front that compresses the upstream's responses (Caddy `encode`) skips
the merge: the compressed body does not parse, and it passes through as is.

That request has the health check's 10 s deadline. While the gateway holds
the upstream frozen, it does not wait at all: it answers 504 at once, with the
reason in `detail` and its own `inference_clients`, whose `frozen_since` says
since when (`null` while the server answers). The same 504 answers a server
that misses the deadline. While frozen, each such request also starts a health
check unless one runs, so polling the route finds the server again once it
answers.

## Inference server (http.rs)

The local inferio orchestrator is an HTTP surface mounted under
`/api/inference` (and, in `panoptikon inferio` mode, at the process root). It
is wire-compatible with the legacy Python `inferio/router.py`; what follows is
the part of it that is about the transport rather than the wire format: the
constants that bound one request and the whole process, why the predict body
is buffered rather than streamed, and what each failure answers.

### Wire formats (Python parity)

Replicated exactly from `inferio/router.py` + `inferio/utils.py`; the
gateway's own `InferenceApiClient` is the parity oracle, and everything the
server encodes must round-trip through it unchanged.

- **predict request** — multipart form with a `data` field holding a JSON
  string `{"inputs": [...]}` (each entry an object, a string, or null, where
  null means a file-only input) and `files` parts whose *filenames* are the
  integer batch indices of the entries they attach to. A filename that is
  missing or not an integer is Python's exact 400 `Invalid index {index} in
  Content-Disposition header`; an empty or absent `inputs` array is 400 `No
  inputs provided`; a missing `data` field is 422, as FastAPI answers a
  missing required Form field.
- **predict response** — exactly one binary output renders as a raw
  `application/octet-stream` body; all-binary outputs render as
  `multipart/mixed; boundary=multipart-boundary` with Python's literal part
  headers (`Content-Type: application/octet-stream`, `Content-Disposition:
  attachment; filename="output{i}.bin"`); anything else renders as JSON
  `{"outputs": [...]}` with bytes entries wrapped as
  `{"__type__": "base64", "content": ...}`.
- **typed per-item errors** (additive) — a batch containing one always takes
  the JSON envelope, since the binary encodings have nowhere to put a typed
  failure, and renders those slots as
  `{"__error__": {"class": "input" | "transient", "message": ...}}`. Absent
  error slots the encoding is bit-for-bit what it always was.
- **`GET /cache/{key}`** — a never-expiring entry (ttl -1) renders as Python's
  `datetime.max.isoformat()` literal `9999-12-31T23:59:59.999999`.
- **errors** — FastAPI's `{"detail": ...}` shape, with router.py's exact
  detail strings for the 500s. The structured object detail below is
  additive; the string form is unchanged for every failure that had one.

Additive query params: `max_batch` on predict (the dispatcher's per-request
item cap) and `prewarm` on load and predict (the lazy-warm hint, absent =
true). `GET /health` has no Python counterpart and lives on the nested
router, with the bare `/health` path also kept in standalone mode.

### Constants

| Constant | Value | Bounds |
| --- | --- | --- |
| `MAX_CONCURRENT_STREAMS` (`main.rs`) | 512 | HTTP/2 streams per connection, advertised in SETTINGS; 8 × the client's 64 per lane, because a reverse proxy fans several clients onto one connection |
| `PREDICT_BODY_LIMIT` | `MAX_FRAME_BYTES` (2 GiB) | one predict request body |
| `PREDICT_INFLIGHT_BODY_BYTES` | 4 GiB | predict body bytes this process holds at once, across every connection and peer |
| `PREDICT_BODY_RESERVE_GRANULE` | 1 MiB | one reservation step for a body that declares no length |

**The per-request limit is `MAX_FRAME_BYTES`** because that is the
orchestrator's own wall on one worker-protocol frame, and it already bounds
the inputs on the way in: `jobs::extraction`'s frame-budget check refuses a
single input above `FRAME_INPUT_BYTES_BUDGET` (that figure minus the
envelope) as a persisted `resource` verdict, before any predict is attempted.
A body above the limit therefore carries either an input this machine has
already decided it cannot infer, or a batch larger than the largest object
either side of the worker protocol ever holds. It is sized for the largest
*legitimate* request — a single maximal input plus a couple of hundred bytes
of multipart envelope — not for "64 inputs per request", which would put the
limit at 128 GiB and bound nothing. Over the limit is `413`, typed
`request_too_large`: re-sending the same batch will not help, and splitting it
will.

**The sender closes a chunk on bytes as well as units.** `REQUEST_UNIT_BUDGET`
= 64 bounds work units, which bound no bytes at all — 64 inputs each admitted
by `FRAME_INPUT_BYTES_BUDGET` are a multi-GiB body that this limit refuses
only after the whole upload has arrived.
`jobs::extraction::REQUEST_BYTE_BUDGET` is 1 GiB of input payload: exactly
`dispatch::MAX_WINDOW_BYTES`, so a byte-closed chunk is precisely one window
(to within 64 B per input: the sender's `input_wire_bytes` counts file bytes
plus the JSON `data`, and the dispatcher's `estimate_input_bytes` counts the
same two plus a 64 B framing allowance) and never a fragment the dispatcher
would have merged — which would read as
queue-bound and hold the ramp down — and half the per-request limit, so a full
chunk still fits with its multipart envelope. An input over the budget on its
own still goes alone; the frame-budget check upstream is what refuses one that
cannot be sent at all, and an input the server still refuses alone is recorded
`resource` — this machine's limit — by that same rule.

**The per-request limit is not a memory bound**, and a per-request limit times
a stream limit is not one either, because nothing bounds how many connections
a peer opens. `PREDICT_INFLIGHT_BODY_BYTES` is the real ceiling: a
process-wide semaphore charged before the bytes are read, from
`Content-Length` where there is one and in granule steps where there is not,
so no body is admitted into memory the process has not already accounted for.
Growth is always *try*, never a wait, so two half-reserved bodies can never
wait on each other, and the reservation is released by `Drop` — a refusal, a
stream failure, a parse failure and a cancelled request all account for
themselves with no explicit release.

**4 GiB, derived from what the shipped client can legitimately offer.** A
gateway job holds at most `[jobs] intermediate_data_budget_mb` (1 GiB by
default) of loaded item data at a time, and those are exactly the bytes its
predict bodies carry. Four times that covers four gateways at the shipped
default against one inference server, more than the deployment this exists
for (a NAS and a GPU box) ever has. It is also twice `PREDICT_BODY_LIMIT`,
which is what keeps the budget from being a trap: the largest request the
server accepts can always be admitted beside another of the same size, so no
legitimate request is ever permanently unadmittable. A compile-time assertion
holds that relation.

The honest worst case: a body being *parsed* is briefly resident twice — the
collected buffer plus the per-field copies taken out of it — so the resident
peak is up to twice the budget, and only if every admitted byte is mid-parse
at the same instant. Steady state for the job this serves is a few hundred
KiB per request over a few hundred concurrent requests, two orders of
magnitude below it.

At the wall the request is refused with `503` and a `Retry-After`, typed
`body_budget_exhausted` so the caller knows the batch was never parsed. It is
never a wait: waiting would hold the stream open — the very thing the
buffered extractor exists to avoid — and would convert an overload into an
unbounded latency instead of an answer. `/health` reports the budget's
`request_limit_bytes`, `budget_bytes`, `in_flight_bytes` and
`refused_requests`; the pair to watch is `in_flight_bytes` against
`budget_bytes`, because a caller refused while the first is far below the
second is being refused by a burst rather than by a level, and the answer is
its own request sizing.

### The buffered multipart extractor

The predict handler collects the whole request body before parsing it. Two
things depend on that.

**The request stream has to reach its end.** A server that answers while the
request body is still open must reset the stream (RFC 9113 §8.1), and hyper
does. The client's terminal DATA frame then lands on a stream this end has
already closed, which h2 reports as a STREAM_CLOSED *stream error* and counts
against `max_local_error_resets` — a counter that only ever rises, for the
whole life of the connection. At 1 024 of them h2 stops the connection with
`GOAWAY(ENHANCE_YOUR_CALM, "too_many_internal_resets")` and every request
body still being read on it fails at once. multer stops at the closing
boundary and never polls the frame after it, so a streamed parse left that
reset behind on every predict — on the one connection a gateway's h2c
self-call keeps for a whole job. Measured over h2c with the real client: 381
of 300 032 predicts failed their parse this way, every one of them surfacing
as axum's fixed sentence `400 invalid multipart body`. Collecting the body
first makes the stream end normally, and the same 300 032 then fail none.

**A transport failure stops looking like a malformed body.** Streamed, "the
connection broke under me" and "these bytes are not multipart" both arrive as
one `MultipartError` whose `Display` is a single fixed sentence with no cause
attached. Collected, they are different code paths: a failed collect is the
body not arriving, and anything the parser says afterwards is genuinely about
the bytes. When the parse does fail, the verdict is asked of the bytes rather
than inferred from a parser error variant — does what arrived carry the
closing delimiter `--<boundary>--` of the boundary this request declared? The
boundary is read through `mime`, the same way multer reads it, so the two can
never disagree; and the scan runs only after the parse has already rejected
the body, so a valid request never pays for it.

### Failure table

| Condition | Status | `detail.kind` | What the caller should do |
| --- | --- | --- | --- |
| Body did not all arrive (collect failed, or no closing delimiter) | 400 | `request_incomplete` | re-submit: nothing was parsed or attempted |
| Body arrived whole and is not a valid batch | 400 | — (plain detail) | fix the request; re-sending is identical |
| No `data` form field | 422 | — | fix the request |
| Body over `PREDICT_BODY_LIMIT` | 413 | `request_too_large` | split the batch and send the halves |
| Process holds `PREDICT_INFLIGHT_BODY_BYTES` already | 503 + `Retry-After` | `body_budget_exhausted` | re-send the same batch shortly |
| Worker process died with the request in flight | 500 | `worker_died` | re-queue the window's items once |
| Model in the load-failure cooldown | 503 + `Retry-After` | `load_cooldown` | do not retry before `retry_at` |
| Model could not be loaded | 500 | — (`Failed to load model`) | router.py parity |

The three `kind`s that mean *this predict never reached a model* —
`request_incomplete`, `body_budget_exhausted`, `worker_died` — are separate
tokens on purpose. They assert the same thing about the items (untouched, so
one re-submission is correct) but name different causes, and a log line that
blames a worker for a broken body sends the next reader to the wrong place.

Responses carry `x-panoptikon-desired-in-flight-items`, the orchestrator's
opinion of how many items the caller should keep inside in-flight predict
requests for that model. It is a header rather than a body field because a
predict answers in three encodings and only one of them has anywhere to put a
scalar; it is additive in all three, ignored by every existing client, and
absent from a Python-era server — which is the "no opinion" case a caller must
already handle. Being a *response* header, the policy layer's inbound
`x-panoptikon-*` strip does not touch it. How the figure is computed is in
`docs/batch-calibration-design.md`, "The in-flight items figure".
