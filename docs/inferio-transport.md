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
is evidence for the memo, not against it. Non-predict calls funnel their send
result through `checked_send` for the same rule, otherwise a memo can go stale
*upward* — a peer remembered as h2c that reappears behind an HTTP/1.1-only
proxy fails `load_model` on every job forever, and a job that fails at load
never reaches the predict that would have cleared the memo.

### Lanes and the stream limit

| Constant | Value | Bounds |
| --- | --- | --- |
| `INFERENCE_CONNECTION_LANES` | 64 | independent h2 connections (sockets) per endpoint |
| `H2_STREAMS_PER_CONNECTION` | 64 | streams offered one lane before the next is recruited |
| `INFERENCE_MAX_CONCURRENT_REQUESTS` | 256 | h2c gate floor; the fixed HTTP/1.1 gate |
| `INFERENCE_MAX_CONCURRENT_STREAMS` | 4096 | h2c gate ceiling (lanes x streams) |

A "lane" is its own `reqwest::Client` with its own pool, which is the only way
to make the number real: for HTTP/2 hyper-util's pool hands every caller the
*same* connection (`Reservation::Shared`, `can_share() == is_http2()`) and its
readiness test is "the dispatch channel is open", not "there is stream
capacity", so a single client never opens a second connection however wide the
window gets. hyper-util also dedups concurrent h2 connects per pool key, so a
burst cannot fan one lane into several sockets.

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

### The in-flight gate

Every admitted request holds a semaphore permit; queued requests hold none, so
a queued request costs nothing where an admitted HTTP/1.1 one costs a socket.

Under **HTTP/1.1** the gate is fixed at `INFERENCE_MAX_CONCURRENT_REQUESTS`
forever and must never follow a model's batching advice: there an admitted
request *is* a descriptor. 256 is four connections' worth. The gate is taken
on both transports because HTTP/1.1 is reachable *after* a job has sized its
window for multiplexing — `in_flight_unit_ceiling` is evaluated once, before
the item loop, so a peer restarted mid-job into a build without HTTP/2 flips
the transport under a window sized for h2c.

Under **h2c** the constant is only the floor. The endpoint publishes a
desired-in-flight figure (`DESIRED_IN_FLIGHT_HEADER`, see
`docs/inferio-worker-protocol.md`) and `set_in_flight_target` follows it,
clamped to `[INFERENCE_MAX_CONCURRENT_REQUESTS,
INFERENCE_MAX_CONCURRENT_STREAMS]`. The floor means this can only ever *raise*
the gate above what every existing deployment already runs at; the ceiling
means a published figure can never make the client offer a lane more streams
than it was designed to, and never moves the descriptor cost at all (bounded
by the lane count, not the gate).

A fixed 256 was justified as "four times a job's in-flight budget (4096 units
at 64 units per request)", which only holds for a model whose items carry 64
units each. An image item carries one, so 4096 units is 4096 concurrent
requests, and 256 was 1/16 of the budget rather than 4x it — a throughput cap
for exactly the models the feature exists for.

The published figure is in *items* and the gate counts *requests*. Using it
directly is conservative in the safe direction: for the models that matter one
item is one request, and for a model packing several units per item it
over-provisions a bound whose only cost is permits, never sockets. The job's
own `UnitBudget` remains the throttle. Several models share one endpoint, so
this is last-writer-wins, which is acceptable exactly because of the floor: the
worst a small model can do to a large one is put the gate back to the constant.

A **shrink never takes a permit away from a request already in flight** — it
withholds permits as they come back (`release_h2_permit`), the same rule as
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
504 and connect, timeout and `REFUSED_STREAM` errors. The lease (gate permit +
lane claim) is dropped before every backoff wait and re-resolved per attempt:
a retry that held its permit across the wait would hold a concurrency slot
while doing nothing, precisely when the server has said it is overloaded, and
a connection error between attempts may have changed the transport.

`REFUSED_STREAM` is reachable in ordinary operation, not only under abuse:
hyper's client opens up to `DEFAULT_INITIAL_MAX_SEND_STREAMS` = 100 streams on
a new connection *before* the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` frame
has been read, and `reqwest` exposes no way to lower that. A burst opened the
instant a lane connects to a peer advertising fewer than 100 has some streams
refused every time until the SETTINGS land. RFC 9113 §8.7 defines it as "not
processed", so it is unambiguously safe to retry. The error chain is walked
for `h2::Reason::REFUSED_STREAM` rather than matched on a string.

A load-failure cooldown (`LOAD_COOLDOWN_KIND`) is the one 503 that must not be
retried: the server is naming when to come back, and a caller that keeps
asking burns the whole cooldown window one request at a time.

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
