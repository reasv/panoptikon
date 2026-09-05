# Inference transport

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
| `MAX_CONCURRENT_STREAMS` (`main.rs`) | 512 | HTTP/2 streams per connection, advertised in SETTINGS |
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
limit at 128 GiB and bound nothing. Over the limit is `413`: re-sending the
same batch will not help.

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
| Body over `PREDICT_BODY_LIMIT` | 413 | — | send a smaller batch |
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
