Panoptikon Gateway (Rust) - Agent Notes

Purpose

- This crate is the HTTP gateway for Panoptikon.
- It is the single entrypoint for UI + API traffic and will gradually reimplement API routes locally while keeping proxy support for remote instances.

Architecture (current)

- Router: Axum routes for `/api`, `/docs`, `/redoc`, `/openapi.json`, `/api/inference/*`, and fallback to UI.
- Proxy: `panoptikon/src/proxy.rs` streams requests to upstreams with minimal rewriting (forwarded headers, URI swap).
- Policy layer: `panoptikon/src/policy.rs` enforces policy selection (by effective host and/or listener endpoint), rulesets, DB param rewriting, and `/api/db` response filtering across both proxied and local handlers.
- Listeners: the primary `server.host`/`server.port` is always the endpoint named "default"; extra `[[server.endpoints]]` entries (`name`, `port`, optional `host` defaulting to `server.host`) each get their own TCP listener serving the identical router. The endpoint name is attached per listener as a `ListenerEndpoint` request extension (an `axum::Extension` layer outside the policy layer) so policies can match on it. All listeners bind before any serves; a failed bind fails startup. The `inferio` subcommand ignores extra endpoints (single listener, tagged "default").
- Local API: `panoptikon/src/api/*.rs` implements `/api/db`, `/api/db/create`, `/api/bookmarks/ns`, `/api/bookmarks/users`, `/api/bookmarks/ns/{namespace}`, `/api/bookmarks/ns/{namespace}/{sha256}`, `/api/bookmarks/item/{sha256}`, `/api/items/item`, `/api/items/item/file`, `/api/items/item/thumbnail`, `/api/items/item/text`, `/api/items/item/tags`, `/api/items/text/any`, `/api/open/file/{sha256}`, `/api/open/folder/{sha256}`, `/api/search/pql`, `/api/search/pql/build`, `/api/search/embeddings/cache`, `/api/search/tags`, `/api/search/tags/top`, `/api/search/stats`, and `/api/jobs/*` locally when `upstreams.api.local = true`. `/openapi.json`, `/docs`, and `/redoc` are served locally when `upstreams.api.local = true`.
- Config: `panoptikon/src/config.rs` loads TOML + env and validates policies/rulesets. `config/server/default.toml` is the single canonical local configuration: primary loopback port 6342 with the API, inference, and supervised UI enabled.
- Host tool/asset discovery (`panoptikon/src/host_paths.rs`): thumbnail fonts, Chromium, and pdfium are resolved without baking `/nix/store` paths into config. Prefer explicit config, then PATH + NixOS profile bins (`/run/current-system/sw/bin`), platform browser installs (macOS application bundles; Windows Program Files/per-user installs), fontconfig (`fc-match`) / `XDG_DATA_DIRS` for fonts, managed-venv `pypdfium2_raw` for pdfium, then classic FHS fixed paths. Successful HTML-renderer discovery is cached only while the executable exists; absence is re-probed so installing a browser can heal blocked HTML files without restarting. HTML capture drives the isolated headless browser over an ephemeral loopback-only DevTools endpoint rather than Chromium's unreliable one-shot `--screenshot` handler; outbound browser traffic remains routed to the dead proxy. Managed-venv PDFium candidates are canonicalized before `dlopen` because hardened macOS processes reject relative library paths. Desktop supplies its separately pinned, bundled PDFium through the existing `PDFIUM_PATH`-templated `jobs.pdfium` setting; on macOS Tauri installs and signs the loadable copy in `Contents/Frameworks` for hardened-runtime library validation. Venv `nodejs-wheel` / `static-ffmpeg` binaries are used only when they actually spawn successfully (skips NixOS stub-ld failures so system/PATH tools win).
- Setup: `panoptikon setup` always re-syncs unless `--if-needed` (skip when the managed venv is complete, the lockfile hash matches, and — with an explicit `--accelerator` — the sentinel's installed extra matches; for idempotent service restarts). `--force` rebuilds the venv and ignores `--if-needed`.
- Config writes: `panoptikon-config` owns lossless TOML/`.env` patching and atomic replacement. Per-index `SystemConfigStore::save` diffs the typed current/requested values into the original document; unchanged comments, order, unknown keys, literal spelling, and absent defaults survive. Desktop uses the same layer for its preferences, Server TOML, file actions, and managed `.env`.

Behavior (important)

- Policy selection: `[policies.match]` takes `hosts` (effective host: `Host`, optionally forwarded headers) and/or `endpoints` (listener endpoint names — physical, header-independent). Empty list = matches anything; both non-empty = AND; at least one must be non-empty. Policies are checked in config order, first match wins, so endpoint-scoped policies belong before broad host policies. The synthesized loopback inference self-call is validated against the primary ("default") endpoint.
- Desktop authority is policy-scoped in addition to managed-mode route mounting: every `/api/desktop/*` request requires the matched policy's `[policies.client] desktop = true`. A separate LAN endpoint can therefore use `allow_all` without inheriting secret reveal or Desktop configuration authority.
- Ruleset allowlisting applies to all API surface paths (`/api/*`, `/docs`, `/redoc`, `/openapi.json`).
- `.env` is loaded at startup for server settings. Inferio additionally reads it just in time before every worker spawn: ordinary Server/Inferio lets inherited env win, while Desktop-managed local inference lets its explicit managed `.env` win. Declared external inputs are validated and explicitly set/removed on the child, so inference changes never require a Panoptikon restart. Desktop external-input management reads the in-process local registry directly (never the possibly remote primary upstream); empty edits keep existing values and only the explicit remove operation deletes them. Remote additive endpoint compatibility ignores only a 404—other discovery failures remain errors.
- On Windows, the gateway sets the executable stack size via linker flags
  (`/STACK:8388608` for MSVC, `--stack,8388608` for GNU) to avoid startup stack
  overflows on the default main thread stack; Tokio worker threads are also
  configured with 8MB stacks.
- `upstreams.api.local = true` means the gateway owns the databases outright: it serves the full API locally (including `/api/jobs/*` and `/api/db/create`), runs the cron scheduler, and runs startup migrations across all on-disk DBs in `data_folder` (skipped when `readonly = true`, matching Python's READONLY). Do not run the Python server's cron against the same `data_folder` in this mode — it would double-schedule extraction jobs.
- Graceful shutdown (`shutdown.rs`): first SIGINT/SIGTERM drains HTTP, stops cron + continuous-scan actors, cancels the running job (queued jobs dropped, new enqueues refused), and flushes index DB writers; 10s cleanup grace, 20s hard exit deadline, second signal exits immediately.
- Desktop supervision: the hidden `--desktop-managed` flag enables parent
  control over stdin (`shutdown` or EOF use the normal graceful path), exposes
  policy-scoped `desktop_managed` in client config (only when that policy's
  client table opts into `desktop = true`), and admits the local-only
  `GET /api/desktop/setup-status`, folder/continuous-validation, and setup-completion
  routes. Readiness evaluates only the
  policy-resolved default index database and reports it ready when a currently
  included folder has a matching `file_scans` row; no separate onboarding
  marker is stored. Folder validation normalizes staged paths without saving
  them; continuous whitelist validation also enforces the staged full-scan
  include/exclude scope. Completion saves file types, both folder lists, the
  staged continuous-scan configuration, the cron model list, and its routine
  schedule; the stored schedule must remain valid even while the routine is
  disabled. It atomically queues an initial folder update followed by the
  selected models; later cron/manual runs use a full rescan instead. Schedule
  preview parses staged cron strings without saving them. Serving processes take an
  advisory `<root>/runtime/server.lock`; lock contention is a clear startup
  error. Ordinary foreground/Server behavior is unchanged.
  Managed bundled invocation materializes missing embedded Desktop configs
  even though `--config` is explicit; files remain create-once/user-owned.
  Its captured console stream is emitted without ANSI styling.
  Desktop also supplies a per-run authenticated loopback shell bridge through
  inherited `PANOPTIKON_DESKTOP_BRIDGE_URL` and
  `PANOPTIKON_DESKTOP_BRIDGE_TOKEN`. Only policies with
  `[policies.client].desktop = true` receive `desktop_shell_available` and may
  use the narrow update status/open/snooze/dismiss routes; the browser never
  receives the bridge token or raw updater capabilities. The bridge URL is
  accepted only as a literal loopback HTTP address with an explicit port;
  bridge requests bypass system proxies and refuse redirects so the bearer
  token cannot leave the process-local channel. Browser mutations require a loopback/`localhost`
  request `Host`, an HTTP `Origin` matching that host, and
  `Sec-Fetch-Site: same-origin` when Fetch Metadata is present; the host
  restriction also blocks DNS rebinding. Snooze and dismissal both carry the
  version the tab displayed; Desktop rejects stale actions after the available
  version changes.
- The supervised production UI receives `PANOPTIKON_API_URL` at build/start
  from the effective gateway listener, so server-rendered UI routes follow
  non-default ports (including the isolated Desktop Dev profile).
- File actions and Relay: `[open]` supports direct `file_program`/
  `folder_program` plus argument arrays, while the older `*_command` shell
  templates remain supported. The Desktop control edits this configuration
  for its managed server. Desktop's loopback Relay service is enabled by
  default; an explicit `enabled = false` in its Relay settings remains an
  opt-out. Policy client key `relay_enabled` defaults true;
  false means the UI must not load or run Relay discovery at all. Pairing
  credentials and bounded operations live in
  `<data_folder>/relay-pairings.json` (owner-only mode on Unix), keyed by
  policy and stable Relay ID. Unfinished operations expire after ten minutes
  and are capped at 256 globally/64 per policy; completed pairings are capped
  at 4,096 globally/2,048 per policy. Browsers retrieve credentials and resume
  operations through one application-wide discovery query;
  `/api/relay/pairings/*` never executes a file action. Relay retains approved
  operations until browser acknowledgement and mapping-blocked actions by
  action ID so Desktop can save a new root and resume them automatically.
- Logging (`logging.rs`): console plus append-mode file, default `<data_folder>/panoptikon.log`; `[logging].file` overrides (empty string disables), `[logging].level` sets the level, `RUST_LOG` wins when set. Routine policy/proxy request-completion events are `DEBUG`; policy denials remain `WARN`, and proxy preparation/transport failures remain `ERROR`, so the default `INFO` level is operational rather than an access log. Config-file string values support env templating (`${VAR}` / `${VAR:-default}`, see `env_template.rs`); global keys reach settings-less code via `config::runtime()` (installed once in main; tests default it to a shared temp root).
- Inference upstreams are configured as an array; the first entry is the proxy + metadata target and may be marked `use_for_jobs = false` to keep it search-only. Extraction jobs only use endpoints with `use_for_jobs = true`. With `[inference_local].enabled = true` the `/api/inference/*` routes are served in-process instead of proxied (see the inferio orchestrator section), and an empty `upstreams.inference` synthesizes a loopback self entry so the gateway's own clients keep working.
- DB param enforcement:
  - Enforces `index_db` and `user_data_db` for DB-aware routes.
  - Strips DB params for `/api/inference/*`, `/api/db`, and `/api/db/create`.
  - Applies allowlists + tenant prefix rewriting via `tenant_prefix_template`.
  - Applies `tenant_default` when present; defaults must be in allowlist.
  - Sanitizes DB names + usernames; unsafe usernames are hashed.
- `/api/db` response filtering:
  - Only allowed DBs are returned.
  - Tenant-prefixed DB names are stripped before returning.
  - `current` values reflect policy defaults (tenant-aware).
- Local DB access:
  - Local handlers use a shared extractor to read `index_db`/`user_data_db` query params.
  - Read-only connections attach `storage` and `user_data` databases, mirroring Python behavior.
  - User-data write handlers open a read-write connection so bookmarks can be updated.
  - Index DB write connections (writer actors) attach `storage` only; `user_data` is not attached for write transactions.
  - SQLite extensions (sqlite-vec) are registered via the bundled Rust crate.
- Index DB writer actors:
  - All Index DB writes are serialized through a per-DB writer actor.
  - Writers open short-lived write transactions and keep a cached connection with a 5-minute idle timeout.
  - A supervisor actor spawns writers on demand and runs 5-minute health checks (index/storage file existence + index/storage read-only ping).
- Job system:
  - `/api/jobs/*` is implemented locally whenever `upstreams.api.local = true`.
  - A global `JobQueueActor` keeps an in-memory queue and running job state; a `JobRunnerActor` executes one job at a time.
  - Job completion flows through a watcher task: it observes the job task ending (return, error, panic, or abort) and sends `JobCompleted` to the runner, which clears its busy state before forwarding `RunnerFinished` to the queue. This ordering means the queue's next `RunJob` can never hit a stale busy state, and a panicking job cannot wedge the queue.
  - Cancellation aborts the job task; extraction item tasks live in a `JoinSet` owned by that task, so they are aborted with it instead of continuing to run and write. Continuous-scan pause/resume uses `JobPauseGuard` (Drop-based), so a cancelled or panicking job cannot leave a DB's continuous scan paused.
  - File scan jobs (`folder_rescan`, `folder_update`) run through `FileScanService` and the index writer actor for writes.
  - Empty included folders are accepted only when the selected index DB has no indexed file rows beneath them. If rows exist, full scans and continuous-watch startup reject the empty root to protect against a temporarily unavailable drive or network share.
  - Data extraction jobs stream items concurrently and serialize all DB writes through the index writer actor. Job `batch_size` caps both the number of items in flight and the total number of work units inside in-flight inference requests (shared unit semaphore); items with more work units than `batch_size` (e.g. many-page PDFs) are split into multiple sequential requests and their outputs concatenated in order.
  - `POST /api/jobs/data/extraction` validates models and resolves effective `batch_size`/`threshold` at enqueue time (mirrors Python): a bad inference ID fails the request, and queue status shows the resolved values.
  - Threshold semantics mirror Python: a zero threshold anywhere in the chain (request, job settings) means "unset" and falls back to the model default; a still-unset/zero final value is omitted from inference payloads so the server-side fallback (e.g. mcut for taggers) applies.
  - Extraction input handlers render PDFs natively via the shared pdfium binding (all pages, 2x scale) and HTML via the shared headless-browser screenshot path — the same code the scan pipeline uses for thumbnails. Render failures fail the item and never write a placeholder, but they are classified and recorded now rather than retried on every cron run forever: a missing backend is `blocked` (self-heals when the dependency appears), a document the backend itself rejected is `input` at the two-attempt threshold.
  - Image inputs get a header-level readability check before upload (mirrors Python `is_image_readable`); unreadable files fail the item instead of reaching the inference server where they could fail a coalesced batch. Deliberately only a header parse: PIL (with truncated images enabled) is the arbiter of still images, and a full decode here would make the gateway stricter than the consumer.
  - Failed media (`docs/failed-media-retry-design.md`, implemented 2026-08-01). Every failure is classified into one `ApiErrorKind`: `transient` (I/O, worker death, server down — never recorded, the item simply fails this run), `input` (the pipeline's own decoder rejected the payload), `blocked` (a missing external dependency, carrying its `blocker`), `resource` (the item exceeds this machine's limits). Persistent classes go into one of three ledgers — `item_extraction_errors` (index.db, keyed (item, setter)), `scan_errors` (index.db, keyed path, with `(last_modified, file_size)` as the retry key), `visual_attempts` (storage.db, keyed (sha256, kind)) — each carrying a `skip_after` confirmation threshold: 1 for a verdict on bytes the gateway had already read, 2 for an external tool that did its own file I/O, where a mount hiccup and a corrupt file are indistinguishable and `attempts` (incremented at most once per run) must reach the threshold before anything is skipped. Nothing expires on a timer: rows die on a content change, on a successful pass, when a missing dependency reappears, or by a shipped retry directive — an ordinary data-only migration named `*_retry_<slug>.sql` whose `WHERE` clause targets class/stage/mime/blocker/setter.
  - Extraction side: `process_item` failures upsert the ledger and count as `input_errors` in the job counters; a *failed ledger write* counts as systemic instead, so a DB outage can never soft-complete as "all corrupt media". The work query gains an internal `NOT FailedFor { setter, min_attempts_reached }` anti-join (a first-class PQL element) so active rows drop out of selection. Job completion is a pure classifier: any success completes normally, all-failed-and-all-input completes with a warning, all-failed with anything non-input hard-fails with the existing inference-server error. `data_log.input_errors` splits the per-job count. Worker responses may carry per-item typed error slots (`docs/inferio-worker-protocol.md`), which is what makes PIL the image arbiter and stops one bad file poisoning a coalesced batch; a merged multi-unit chunk that fails is re-submitted unit by unit once. Worker-reported input errors for text-entity models stay transient (the ledger keys on the item, not the data row).
  - Scan side: at the start of every root the walker preloads that root's `scan_errors` rows into a map (one full read of a table that is a handful of rows by design, scoped to the root in Rust because containment and case-fold rules cannot be expressed as an index range; empty for essentially every scan), skips a path whose row is active and whose stat still matches, keeps skipped paths out of unavailable-marking, deletes the row when the path processes successfully, and sweeps rows the walk never reached — deferring the sweep entirely when a directory could not be read. The continuous scan uses the same table through one point lookup per event plus a session cache of failed stats. The scan's two-tier policy is uniform across types except intentionally opt-in HTML: ordinary **metadata** failures mean not indexed (`mime`, `metadata`, `header` rows), ordinary **visuals** failures mean indexed without visuals (`visual_attempts` markers), but an HTML file is useful only with its rendered page, so no renderer or any failed first screenshot prevents insertion; persistent failures are promoted to metadata-stage scan failures. Existing HTML items are not removed merely because a browser later disappears. Images follow the ordinary split because the fused decode was un-fused — the metadata phase parses only the header (dimensions, and the same check extraction makes), the full decode happens in the visuals phase, and a `decode` row is therefore *audit-only*: it never suppresses a re-attempt and is not spent by its own file's success, since that file now indexes on every scan.
  - Both scan-side tables auto-heal `blocked` rows in one pass at scan start, probing only the distinct blockers actually present. HTML renderer absence is not negatively cached, so a newly installed browser can clear waiting HTML rows on the next scan without a gateway restart; other backend cache lifetimes retain their own behavior. Extraction jobs use the same blocker probes for their ledger.
  - Audit surface: `GET /api/jobs/data/failures` and `GET /api/jobs/scan/failures` (filterable by setter/class/stage/mime prefix, paginated, each response carrying the unpaginated total), surfaced as the job page's "Failed files" card. `visual_attempts` has no endpoint yet — a visuals failure is visible only through the `scan_errors` audit row images write and through the logs.
  - Sliced image inputs are re-encoded in their source format (PNG stays PNG with alpha; unknown formats fall back to PNG); JPEG slices keep the quality-85 encoder. PDF pages and HTML screenshots are sliced using their own rendered dimensions, other frames use the item's stored dimensions.
  - Tag output text entries keep Python's ordering: namespaces in first-appearance order, tags confidence-sorted within each namespace. Empty `metadata` objects produce no metadata text entry.
  - `data_log` start and end times use the same local-time format (`db::extraction_write::current_iso_timestamp`), and incomplete-job cleanup runs before the remaining count so `[jobs].atomic_extraction_jobs` cleanup is reflected in it.
  - File scan jobs honor `filescan_filter` (PQL `Match`) during stage-1/2 file filtering and apply `job_filters` entries that include `file_scan` after scans to delete files that violate the rules.
  - Queue status lists the running job first with `running=true`, followed by queued jobs, and includes a bounded process-local `outcomes` list for the 256 most recent completed, failed, or cancelled jobs. Desktop setup uses those outcomes to distinguish successful completion from failure instead of inferring it from queue disappearance.
  - Queue cancel can target queued jobs and the running job (best-effort cancellation). Both cancel routes take `run_maintenance` (default true); `false` suppresses only the deferred-maintenance job that *this* cancel's boundary would synthesize — the owed flags survive for the next boundary.
  - Per-DB maintenance (optional VACUUM, tag recount, ANALYZE, WAL checkpoint) is deferred to the job boundary instead of running inside every job. Jobs report a `ChangeSummary { wrote_data, deleted_data, tags_changed }`; the queue ORs it into `owed[index_db]` (`merge_owed` derives `tags_changed` from `deleted_data`, since deletions cascade into `tags_items`). A job that ends without reporting (cancelled, panicked, failed) is assumed pessimistically to have written, and to have deleted when its type cascades deletes (folder rescan/update, data deletion, job-data deletion); `vector_quant_reconcile` is exempt from the pessimistic rule entirely (it writes only quant tables and never owed maintenance). A long job that commits deletions before it can fail — an extraction job's embedded folder resync — reports them mid-flight with `queue::record_owed_now`, so the debt outlives a later failure or cancel. When a job leaves the running slot and no queued or running job targets its DB, the actor synthesizes a `db_maintenance` job at the *back* of the queue and clears the owed flags — visible in queue status, cancellable, and serialized like any other job. Back, not front: maintenance can run for minutes (VACUUM, and a recount that scales with `tags_items`), so at the front it would stall the GPU pipeline mid-drain and outlive the 60 s batch-model TTL; at the back it accumulates at the tail and runs after the last extraction. A drained queue starts it immediately either way. The skip-over-maintenance rule in `next_batch_setter` is kept as belt-and-braces. Cancelling it does not re-own its flags. Owed flags die with the process (the queue is not persistent); shutdown never synthesizes.
  - `db_maintenance` jobs never fail: step failures are logged, matching the contract the in-job maintenance pass always had. Their `metadata` is the owed-flag list (any of `wrote_data`, `deleted_data`, `tags_changed`), which is also what tells the job whether it may VACUUM and whether it must recount.
  - The tag recount is gated; ANALYZE and the WAL checkpoint still run whenever a maintenance job runs. It runs when the owed `tags_changed` flag is set **or** the DB's durable tags-dirty marker is set (`maintenance_state.tags_dirty`, one row, migration `20260728120000_maintenance_state.sql`, seeded dirty so an upgraded database recounts once). The marker exists because jobs are not atomic: a shutdown mid-tagging job has already committed tags that no surviving in-memory flag remembers. It is set inside the same transaction as the write that dirties the counts, by **every** writer handler that can remove rows the counts depend on: `WriteTagsOutput` (first *content-carrying* write of a writer session — latched in writer state so per-tag writes cost nothing, a respawned writer pays one redundant upsert, and `write_placeholder`'s empty write never marks), `DeleteItemIfOrphan`, `DeleteItemsWithoutFiles`, `DeleteJobData`, `DeleteSetterData` and `RemoveIncompleteJobs` — each only when it actually deleted something. File-only and blob-only deletions (`DeleteFileByPath`, `DeleteUnavailableFiles`, `DeleteFilesNotAllowed`, the folder deletions, `DeleteOrphanedFrames`/`Thumbnails`) never touch `tags_items` and do not mark. On top of that the boundary casts `MarkTagsDirty` when owed `tags_changed` first flips true — a backstop for the pessimistic flags of a job that ended without reporting; it is skipped when that same boundary schedules a pass that will recount anyway, and the `Shutdown` handler writes it *awaited* (2 s timeout) instead of detached, since a task spawned as the runtime tears down is usually never polled. Only a successful `RecountTagItems` clears it, in the same transaction as the rebuild, and that also resets the writer latch. A failed marker read recounts (the pre-gate behavior).
  - `POST /api/jobs/maintenance` enqueues a `db_maintenance` job with every flag set for the selected DB (202 + `JobModel`), at the back of the queue like all maintenance, and clears that DB's owed flags exactly as synthesis does (an all-flag pass covers them; leaving them set would make its own boundary schedule a second full pass). A maintenance job already *queued* for the DB is upgraded in place to all flags and returned instead of adding a second one — a pass the boundary synthesized with only `wrote_data` would not recount, which is what the request asked for. 409 only when one is already *running*, since its flags were read when it started. It reaches the queue through a dedicated `EnqueueMaintenance` actor message, not through `JobRequest` — synthesis and its owed-flag bookkeeping remain the only other way a maintenance job can exist.
  - VACUUM is additionally gated on measured free pages: `PRAGMA freelist_count` / `page_count` on both `main` and the attached `storage` database; it runs only when either has `>=10%` free pages **and** at least 2,500 of them, or at least 250,000 free pages regardless of ratio (~1 GB at 4 KiB pages). The ratio is the primary signal — a bare absolute floor would fire on every deleting job once the database is large. A failed measurement vacuums (the pre-gate behavior). The same gate applies to the continuous scan's periodic maintenance.
  - Batch models survive between extraction jobs. Extraction jobs no longer unload at the end; they report the setter they loaded (`JobSuccess.loaded_model`, `None` on the no-data early return) and the queue tracks it as `batch_loaded` — exact, because batch loads use `lru_size = 1` under cache key `batch`, so loading one setter evicts the previous. At every boundary the queue finds the first queued `data_extraction` job, skipping over synthesized `db_maintenance` jobs **only** (any other job type ends the run — a scan can take hours and the model should not hold VRAM through it), and unloads unless that job's `metadata` matches `batch_loaded`; an empty queue always unloads. The unload is fire-and-forget (`tokio::spawn`), never blocking the actor on an HTTP call, but it is **generation-guarded**: loads run under a shared batch slot (`extraction::lock_batch_slot`) that bumps a load generation, and a spawned unload captures that generation and aborts if a newer load has taken the slot — without it a late unload could land after the next job loaded the same setter and fail everything queued on that dispatcher. An extraction job that ends without reporting (cancel, failure, panic) is assumed to have loaded its `metadata` setter (`setter_name == inference_id == metadata`), so the cancel path gains an explicit unload it never had. Shutdown unloads the cancelled job's model too, awaited with a 2 s timeout instead of detached (a task spawned as the runtime tears down is usually never polled). Every path that loses the call still falls back to the inferio TTL sweep (60 s TTL, 10 s sweeper), which has always been the real guarantee here.
  - Cron jobs are fully ported (`jobs/cron.rs`): a scheduler actor ticks every minute over all index DBs, evaluating each DB's `cron_schedule` (croner, croniter-compatible 5-field patterns, local time) with Python's semantics — config re-read every tick, a changed string recomputes the next fire from now, no catch-up for missed runs (deliberate: startup must never kick off a GPU-heavy run on its own). The scheduler starts whenever `upstreams.api.local = true`.
  - `run_cronjob` (shared by the scheduler and the manual trigger, which deliberately ignores `enable_cron_job`) enqueues a folder rescan first, then extraction jobs ordered items/files-targeting models before derived-data models; all tagged `cronjob`. The batch is enqueued atomically and skipped while a previous cronjob for that DB is queued/running (dedup lives inside the queue actor to avoid check-then-enqueue races). A model unknown to the inference server is skipped; if the metadata fetch itself fails, jobs are enqueued unordered instead of consuming the slot (deliberate improvement over Python).
  - A scheduler tick runs in two passes: every DB's schedule is ticked first, then all DBs that fired in that tick are built and enqueued as **one** merged batch (inference metadata is fetched once per tick, not once per DB). `merge_cron_batches` is a stable sort on `(phase, setter first appearance within that phase)` where phase is scan / source-model / derived-model, so same-setter jobs from different DBs come out adjacent and reuse the loaded batch model, while each DB's dependencies — which are entirely expressed by the phase — still hold. Two jobs of one DB in the same phase have no ordering requirement, so setter grouping may interleave them with another DB's. Reordering only ever happens inside a single enqueue; already-queued jobs are never touched. Staggered cron times land in different ticks and do not merge. One enqueue call covers all fired DBs, so a queue-unavailable error costs all of them the slot their schedule already consumed (both error sources are global anyway).
  - When the inference metadata fetch fails, models get a fourth phase, `Unknown`, which sorts after the scans and is ranked by (batch, position) rather than by setter: the DBs' blocks are **concatenated**, each keeping its config order, and no setter grouping happens. Without a classification the phase carries no dependency information, so grouping could otherwise run one DB's derived model before the source model that feeds it — the price is losing cross-DB model reuse in exactly the tick where it cannot be proven safe. Nothing is dropped; extraction jobs re-resolve metadata when they run.
  - Batch dedup (`EnqueueBatch.dedups`) is evaluated per index DB: a conflicting entry drops only that DB's requests and names it in `skipped_dbs`; the rest of the batch still enqueues atomically. One stale cronjob therefore cannot hold back the other DBs in a merged tick. `CronRunOutcome::Skipped` is "this DB is in `skipped_dbs`"; single-DB callers (manual trigger, desktop wizard, vector-quant reconcile) pass one dedup and read it that way.
  - `PUT /api/jobs/config` rejects unparseable `cron_schedule` strings with 400 (Python accepts them and fails silently in the ticker). `GET /api/jobs/cronjob/schedule` (additive, not in Python) reports enabled/valid/next_run/last_run.
  - Embedding-model preload (`preload_embedding_models`) runs on the same minute tick, mirroring Python: existing text-embedding/clip setters (excluding `tclip/`) are kept loaded under cache key `preload[<index_db>]` with 1h TTL and renewal ~2 minutes before expiry; disabling clears the inference cache once.
  - System config parses `job_filters` and `filescan_filter` as PQL objects; invalid PQL in config fails to load (mirrors Python).
- Accelerator report (`panoptikon/src/accelerator_report.rs`): at server and
  `inferio` startup (and via `panoptikon accelerator`) we always log the
  resolved inference **backend** (`cpu` / `cuda` / `rocm`, extensible) and any
  detected GPU names (with compute capability where nvidia-smi reports it).
  Backend priority: managed-venv sentinel → config/`auto` host probes. There
  is deliberately no separate env-var resolution path: `PANOPTIKON_ACCELERATOR`
  (Nix wrap, packagers) reaches the config value through the
  `${PANOPTIKON_ACCELERATOR:-auto}` template line in the shipped TOML, like
  every other env-bridged setting. GPU stacks
  are pluggable probes (NVIDIA + AMD/ROCm today; add Intel XPU etc. later).
  ROCm device names prefer `rocm-smi`; fallback `rocminfo` keeps only agents
  with `Device Type: GPU` (CPU agents also have a Marketing Name and must not
  be listed as devices). **CPU** backend is logged as using CPU (no warning).
  A **GPU** backend with no device name yields a **warning**, not a failure.
- Accelerator env (`panoptikon/src/accelerator_env.rs`): workers get host
  HIP/HSA on `LD_LIBRARY_PATH` only when the **installed** accelerator is
  `rocm` — read from the setup sentinel's `extra=` line
  (`setup::installed_accelerator`), the ground truth for what `uv sync` put
  in the managed venv. Config-based resolution
  (`setup::effective_accelerator`) is only the fallback for user-managed
  interpreters and legacy venvs (no sentinel); explicit `cpu`/`cuda` never
  inject. Also sets MIOpen defaults when unset:
  `MIOPEN_FIND_MODE=FAST` and cache dirs under
  `$XDG_CACHE_HOME/panoptikon/miopen` (avoids EasyOCR/CRAFT stalls from
  GemmFwdRest workspace=0 solver search). `probe_after_setup` runs post-sync
  checks (ROCm torch HIP probe today; no-op otherwise).
- Inferio orchestrator (`panoptikon/src/inferio/`), the Rust port of the Python inference server: `registry.rs` parses the inference TOML registry into per-id spawn specs; `worker.rs` supervises `python -m inferio_worker` child processes speaking the framed-msgpack protocol (`docs/inferio-worker-protocol.md` v2) — handshake (worker *identity* only: `protocol_version=2` + `impl_class` + `impl_dirs`, no instantiation; a version echo != 2 is a fatal kill), optional `prewarm` (runs the impl's optional `prepare()` classmethod between handshake and configure; idempotent, errors per-request and non-fatal; uses the LOAD deadline since prepare exists to pay the slow imports early), `configure` (binds a concrete model: instantiates `impl_class(**config)`, exactly once, before load; errors are per-request and do NOT poison the worker), then load/predict/ping/unload (unload valid in every state — a parked prewarmed worker exits 0 the same way). `Worker::spawn` does handshake only; `Worker::spawn_configured` chains spawn+configure for the normal flow (what `manager.rs::spawn_model` uses). Lifecycle deadlines per the protocol doc (handshake deadline covers configure/ping; prewarm gets the load deadline), single outstanding request enforced via `&mut self`, stderr forwarded to tracing with a bounded tail attached to error reports, per-request `error` frames surfaced as downcastable `WorkerError` (worker survives), framing violations/timeouts/exits treated as fatal (worker killed + poisoned), and graceful stop via the unload → terminate → kill ladder. Workers sit under `kill_on_drop` plus the shared kill-on-close Job Object (`panoptikon/src/process_tree.rs`, extracted from `jobs/files.rs` and also used by the HTML-thumbnail browser path).
  - `manager.rs` ports the legacy Python `inferio/manager.py` (python-legacy branch) exactly (design doc §5): per-cache-key insertion-ordered LRU with `lru_size` enforced on load (oldest evicted first), cache-key refcounts (a model unloads only when its last reference disappears), TTL `>= 0` = now+ttl / negative = never, a sweeper task (config `sweep_interval`, Python: 10 s), and repeated load renewing TTL + LRU position (cron preload depends on this). Predict auto-loads, then pins the model via refcount for its duration (design §5 delta: overlapping predicts can't unpin each other) and restores the requested TTL afterwards. Deliberate deviations (documented in the module docs): failed loads never leave phantom `/cache` ids, `lru_size <= 0` refuses the load instead of leaking a process, explicit unload lets an in-flight batch finish, and the post-predict TTL restore doesn't re-run the full load path. Loads are serialized by an async `load_lock` (mirrors Python's manager-wide lock); bookkeeping lives under a std mutex never held across await. Fatal worker death fails all queued requests, drops the model from all LRUs (generation-guarded), and the next predict respawns.
  - `dispatch.rs` implements dispatch-time batching (design §6) over a multi-replica WorkerSet (design §8, Phase 3): per model, a plain tokio task + mpsc queue owns N worker replicas serving ONE shared FIFO queue — free replicas sit in a pool, in-flight windows run as `JoinSet` tasks that return their replica to the pool, and whenever any replica is free the queue is drained into a window for it, merged FIFO up to `effective_max_batch` = max over *explicit* `max_batch` values in the window (cap-less requests contribute no opinion — the OOM-recovery property), falling back to registry metadata `default_batch_size` (group overlaid by id) and then the server default (`ManagerConfig::default_max_batch`, replaces `MAX_COMBINED_BATCH`). Request *pickup* is strictly FIFO (windows are queue prefixes); completion order across replicas may differ (per-request oneshot replies). Oversized single requests are split into sequential sub-batches; a merged batch failing with a `WorkerError` falls back to per-request prediction on the same replica (port of `process_model.py::_batch_predict`).
    - WorkerSet shape comes from the registry TOML: per-id (group-inheritable like any config key) `config.devices = ["3", "7"]` → one replica per entry, pinned `CUDA_VISIBLE_DEVICES=devices[i]` at spawn; `config.replicas = N` alone → N replicas pinned `"0"`..`"N-1"`; neither → 1 replica, no pin. Both keys are stripped from spawn kwargs like `ray_config`; both given with mismatched lengths (or malformed values) is a registry *load* error. `spawn_model` spawns+handshakes+configures+loads all replicas concurrently — any replica failing kills the others (whole-set load atomicity, no partial sets).
    - Death policy (deliberate Phase 3 choice; degradation to a smaller set is future work): ANY replica fatal → the whole model dies — queued requests failed, in-flight windows on other replicas aborted (their callers error; dropped workers reaped by kill_on_drop + Job Object), idle replicas killed ladder-less, `handle_worker_death` once under the generation guard. Unload/shutdown runs the graceful unload → terminate → kill ladder on all replicas concurrently after in-flight windows finish.
    - Test fixtures for manager behavior live in `python/tests/inferio_worker/fixture_impls/` (`slow_test`, `batchsize_test`, `failbatch_test`, `dying_test`, `device_test` — echoes its `CUDA_VISIBLE_DEVICES`, `external_env_test` — echoes a declared just-in-time worker env value, `dieflag_test` — hard-exits on `{"die": true}` input, else sleeps 1s and echoes; v2 protocol fixtures: `prepare_test` — prepare() classmethod sets a module flag + stderr marker, predict reports the flag, `prepare_fail_test` — prepare() raises). `python/tests/inferio_worker/fake_v1_harness/` holds a fake stale harness (answers the handshake with protocol_version 1) used by worker.rs' version-mismatch kill test via a PYTHONPATH prepend.
  - `http.rs` is the wire-compatible HTTP surface (port of the legacy Python `inferio/router.py` + `utils.py`, design §7), gated by `[inference_local].enabled` in gateway config (default false):
    - Routes (mounted via `nest_service` under `/api/inference`, behind the same policy layer): predict/load/cache, `GET /metadata`, additive `GET /external-inputs` (reusable declarations, per-ID requiredness, presence only), and additive `GET /health`. Registry `config` templates remain raw until `spawn_spec`; external-input declarations, not template discovery, drive validation. Models with declared worker env do not claim generic prewarmed processes. See `docs/inferio-external-inputs.md`.
    - `GET /health` (design §7) returns `ModelManager::health()`'s `HealthReport`: top-level `status` ("ok"/"shutting_down"), `shutting_down`, `registry_ok` (cheapest correct check: mtime-gated `RegistryCache::get()` — a stat scan unless a config file actually changed; broken TOML shows `false` without disturbing loaded models), `model_count`, and `models[]` sorted by id with `inference_id`, `generation`, `cache_keys` (sorted), `replicas {total, free}`, `queue_depth`, `in_flight_windows`, `last_effective_cap` (null until the first window dispatches), `total_predict_requests`, `total_batches`. Backed by per-model `ModelStats` (dispatch.rs): Relaxed atomics shared dispatcher (writer) / manager (reader) — the hot path pays one uncontended store per event, no locks; readings are advisory snapshots. Plus a `prewarm` section: `{enabled, lazy, warm: [{impl_class, state}]}` with state `"warm"` (parked), `"spawning"` (background warm-up in flight), or `"failed_prepare"` (parked but `prepare()` raised — claims still work, load pays the imports).
    - Wire formats are byte-parity with Python: predict request is multipart form (`data` = JSON string `{"inputs": [...]}`, `files` parts named by integer batch index in the filename); predict response is `application/octet-stream` for a single binary output, `multipart/mixed; boundary=multipart-boundary` with Python's literal part framing for all-binary outputs, else JSON `{"outputs": [...]}` with bytes wrapped as `{"__type__": "base64", "content": ...}`. `GET /cache/{key}` renders ttl -1 as `9999-12-31T23:59:59.999999` (Python `datetime.max`). Errors use FastAPI's `{"detail": ...}` shape with router.py's exact 500 detail strings ("Failed to load model", "Prediction failed"). The body limit is disabled on these routes (the proxy path had no cap). The round-trip test in `http.rs` drives the routes with the gateway's own `InferenceApiClient`, which is the parity oracle.
    - Config `[inference_local]`: `enabled` (default false), `python` (default auto-detect the managed venv `python/.venv/Scripts/python.exe` / `python/.venv/bin/python` relative to CWD, falling back to the legacy root `.venv`; missing interpreter is a load-time *warning* — workers spawn lazily), `impl_dirs` (default `["python/inferio/impl", "inferio_custom"]`; the config key is the only override — the old env fallbacks are gone. There is no local-mode analogue of `INFERIO_ALLOW_BUILT_IN_OVERRIDE`: dirs are searched in order, built-ins first, customs later), `config_dirs` (default `["python/inferio/config", "config/inference"]`; registry TOMLs are env-templated on every load/reload), `pythonpath` (default `["python"]`), `default_max_batch` (32), `sweep_interval_secs` (10), optional worker deadline overrides (`handshake_secs`, `load_secs`, `unload_grace_secs`, `terminate_grace_secs`), `port` (used only by the `inferio` subcommand), and the `[inference_local.prewarm]` sub-section: `enabled` (default true), `lazy` (default true), `always_warm` (impl-class list, default empty).
    - Loopback synthesis rule: when `inference_local.enabled = true` and `upstreams.inference` is empty, a loopback self entry (`http://127.0.0.1:{server.port}`, IPv4 wildcard hosts mapped to 127.0.0.1, IPv6 wildcards to `[::1]`) is synthesized so jobs/PQL/cron-preload/UI work with zero config — they talk HTTP to the gateway itself, *through the policy layer*: config load verifies a policy matches the synthesized host and admits the inference routes (predict/load/metadata), failing fast with remedies instead of letting every self-call 403 at runtime. When `upstreams.inference` is non-empty it is left untouched (mixing local + remote endpoints is allowed; entry order still decides who serves search/metadata and jobs). When local inference is disabled the old default applies (API upstream).
    - Subcommand: `panoptikon inferio [--config ...]` starts ONLY the inference service (design §3 GPU-lender mode): `/api/inference/*` (including `/api/inference/health`) plus bare `GET /health` (same handler/shape as `/api/inference/health` — kept so existing probes of the subcommand path keep working; the old `{"status": "ok", "loaded": {...}}` body is superseded by the `HealthReport` shape), same config load and policy layer, no proxy/local API/jobs/cron/migrations. `inference_local.enabled` is implied; `[inference_local].port` overrides the listen port (default `server.port`). `--config` is a global clap arg so it works after the subcommand.
    - Prewarm pool (`prewarm.rs`, design §8, policy decided 2026-07-05): one parked worker per **impl class** (spawned, v2 identity handshake, `prewarm` sent — a failed `prepare()` parks anyway), owned by `ModelManager`, no TTL ever (its purpose is to outlive the loaded model's TTL). Claim happens in `spawn_model` for at most one replica per set (the first *unpinned* one — pooled workers spawn without `CUDA_VISIBLE_DEVICES`): pool slot removed → `ping` → alive: `configure` + `load` it; ping-dead: discard + fresh `spawn_configured` (a fatal error *between* ping and configure also falls back to a fresh spawn; a `WorkerError` from configure propagates — a fresh spawn would fail identically). Lazy rule: after any successful model load (claim or fresh), if master+lazy switches are on and the request's `prewarm` hint != false, a background warm worker of that class is (re)spawned — respawn-on-claim is this same rule. Eager set: `prewarm::run_eager_prewarm_loop` (spawned by main.rs in gateway mode only, when `inference_local.enabled && prewarm.enabled`) enumerates index DBs at startup + every 60s; per DB with `SystemConfig::prewarm_embedding_models` (default true, Rust-only field like `continuous_filescan`), selects search-usable embedding setters WITH DATA via the shared `db::extraction_log::get_search_embedding_setters` (the exact cron-preload filter: text-embedding/clip, excluding `tclip/`), maps setter → impl class via the registry, unions `always_warm`, and ensures warm workers; per-DB failures log + skip. `always_warm` warms at `ModelManager::new` in every mode — it is the only eager mechanism in the `inferio` subcommand (no DB scan there). Pool ops never run under the manager state mutex (own mutex, background spawn tasks); parked workers get the graceful unload ladder during `ModelManager::shutdown`, concurrently with dispatcher drains (in-flight warm-up tasks are aborted; their children reaped by kill_on_drop + Job Object).
    - Shutdown: `ModelManager::shutdown` (refuse new loads, fail queued predicts, per-worker unload → terminate → kill ladder) is hooked into `shutdown.rs` cleanup after the job queue stops (jobs are the main predict callers) and after the index-writer flush (the predict path writes nothing to index DBs once the queue is stopped, and a wedged GPU batch must not starve the flush), inside the 10s cleanup grace; a wedged worker past the 20s hard exit is still reaped by the kill-on-close Job Object. The `inferio` subcommand uses a reduced cleanup (`run_inferio_cleanup`) with the same grace/force-exit envelope.
- Local DB migrations:
  - SQLx migrations live in `panoptikon/migrations/index`, `panoptikon/migrations/storage`, and `panoptikon/migrations/user_data`.
  - `db::migrations::migrate_databases` can create or update on-disk DBs and supports in-memory DBs for tests.
  - Existing Python-created DBs without `_sqlx_migrations` are baselined to the first migration so future migrations can apply. Baselining is guarded: the DB's `alembic_version` must equal the head revision the init snapshot was taken from (constants in `migrations.rs`), otherwise startup fails with an explicit error. Freshly created DBs get the alembic head stamped into `alembic_version` so Python can still manage them during the transition.
- Local PQL search:
  - `/api/search/pql` compiles queries via the Rust PQL builder and executes them locally.
  - `/api/search/pql/build` returns the compiled SQL/params without executing.
  - Extra columns use the Rust alias map, and `check_path` results are validated with fallback file lookup.
  - When `check_path` is enabled for `entity = file` and no `partition_by`, missing paths are dropped without substitution (matching Python behavior).
- Streaming:
  - All responses are streamed except `/api/db`, which is buffered so it can be filtered.

Motivations (why it is built this way)

- Keep policy enforcement in one place (layer) so local handlers can mirror upstream behavior.
- Preserve proxy compatibility even as more routes are implemented locally.
- Support multi-tenant DB selection safely and consistently across all API calls.

Tests

- Most behavior is tested in `panoptikon/src/policy.rs` under `mod tests`.
- When adding policy/DB rules, add unit tests there using `axum::http::Request` to validate query rewriting and response filtering.
- When adding local routes, add focused tests for handler outputs plus policy layer behavior if it transforms responses.
- All tests must include a descriptive comment above each test explaining expected behavior and outcomes.

When you change behavior

- Update this file to document new behavior, config knobs, and any new routes or policy rules.
- Keep the "Behavior" section authoritative; if behavior changes, update it.
- If the policy layer or proxy flow changes, also update `panoptikon/README.md`.
- Keep DB connection/helpers/CRUD code inside `panoptikon/src/db/`.

PQL Rewrite (Rust, Planned)

- Goal: fully replace the Python PQL compiler with a Rust implementation that is behaviorally identical for both results and performance-critical SQL structure.
- Rollout: gated by an explicit experimental env flag; when enabled, Rust PQL is the only path (no proxy fallback, no shadow mode).
- OpenAPI: PQL types are annotated for OpenAPI generation from the start; when `upstreams.api.local = true`, `/openapi.json` is served from the Rust generator even though some endpoints are still proxied.
- Architecture:
  - Schema/AST: `serde` models mirror the Pydantic union shapes and field names (`and_`, `or_`, `not_`, filter fields).
  - Preprocess/validation: matches Python behavior exactly, including filter-specific mutations (e.g., `MatchText.filter_only`).
  - Builder: SeaQuery-based query builder replicates `QueryState`, CTE chaining, root CTE unwrapping, join ordering rules, `order_by` + `partition_by`, and extra-column handling.
  - Join tracking: filters record which base tables they already join so root CTE unwrapping does not introduce duplicate base-table joins (avoids ambiguous column errors).
  - Count queries: preserve count semantics (including partition-by counting and ignoring gt/lt cursor filters).
  - SQLite specifics: FTS5 `MATCH`, `snippet(...)`, and vector functions are emitted as raw SQL fragments where needed.
- Initial filter subset (fully working core):
  - `Match`, `MatchPath`, `MatchText`, `MatchTags`, `InBookmarks`, `ProcessedBy`, `HasUnprocessedData`.
  - Embedding filters (`SemanticTextSearch`, `SemanticImageSearch`, `SimilarTo`) are implemented and require async preprocessing with the inference API for embeddings + distance function overrides.
- Implementation status:
  - `Match` is implemented with KV joins + recursive operator handling (eq/neq/in/nin/gt/gte/lt/lte/startswith/endswith/contains, plus nested and/or/not).
  - `MatchPath` is implemented with FTS5 `MATCH`, `rank`-based `order_rank`, `row_n` windowing, and `gt`/`lt` cursor filtering.
  - `MatchText` is implemented with FTS5 `MATCH`, setter/language/confidence filters, snippet extraction, and `row_n` windowing for best snippet selection.
  - `MatchTags` is implemented with tag/name/namespace filters, setters, confidence thresholds, and exact/all-setter matching via HAVING clauses.
  - `InBookmarks` is implemented with user + namespace filtering (including sub-namespaces) and ordering by latest bookmark timestamp.
  - `ProcessedBy` is implemented with setter filtering over derived data per item/data row.
  - `HasUnprocessedData` is implemented with derived-data `NOT EXISTS` checks and placeholder filtering.
  - `SemanticTextSearch` is implemented with embeddings distance aggregation (MIN/MAX/AVG), optional source-text filters + weights, and per-entity join paths.
  - `SemanticImageSearch` is implemented with CLIP cross-modal support, source-text filters, and model distance-function overrides.
  - `SimilarTo` is implemented with an `unqemb` CTE, cross-modal constraints, and weighted distance aggregation when source-text weights are provided.
  - `preprocess_query_async` embeds queries via the inference upstream and loads model metadata for distance-function overrides; the sync preprocessor accepts base64 embeddings or prefilled `_embedding` fields.
  - Inference metadata is cached per inference base URL (5-minute TTL) to avoid repeated `/metadata` calls during preprocessing.
    - Callers that need fresh metadata can construct the client with caching disabled (`InferenceApiClient::from_settings_with_metadata_cache(..., false)`).
- Search-time embeddings are cached in-process with a global LRU keyed by `(model, kind, query)`; cache size is controlled by `search.embedding_cache_size` in gateway config and defaults to 1,024 entries.
  - `/api/search/embeddings/cache` provides cache stats and allows clearing the embedding cache.
  - Embedding decoding accepts `f16/f32/f64`, integer/boolean dtypes, and both C/Fortran order; non-float inputs are coerced to `f32` and 2-D arrays use the first row.
  - Inference predict calls (multipart uploads) bypass the retry middleware and use a raw reqwest client with manual retry logic because multipart bodies are not clonable.
  - Inference embed/metadata errors are sanitized in client responses; detailed error context is logged server-side.
  - `/api/search/pql` and `/api/search/pql/build` now use the Rust compiler; no Python PQL calls remain in the gateway.
  - Test strategy (results + performance invariants):
    - Use Python `/api/search/pql/build` as the reference compiler for fixtures during development when needed.
    - Validate result equivalence and ordering on a fixed SQLite fixture DB.
    - Per-filter unit tests build a full PQL query and execute it against in-memory test databases to ensure the generated SQL is valid for our schema.
    - Validate SQL structure without relying on byte-for-byte SQL equality:
      - Normalize SQL (whitespace/casing) and compare key structural properties (CTE ordering, join graph, selected columns).
      - Track query plans as a diagnostic signal; do not rely on plan output alone, but use it to spot regressions in join/index usage.
  - Maintain a golden fixture suite covering all implemented filters, text vs file entities, partitioning, and ordering edge cases.

Continuous File Scanning (Implemented)

- Scope: optional continuous file scanning per index DB, controlled by `[continuous_filescan].enabled = true` in the per-DB TOML SystemConfig. This feature is not part of the job queue.
- Actor topology:
  - `ContinuousScanSupervisor` (singleton actor) maintains `index_db -> ActorRef<ContinuousScanActor>`.
  - One `ContinuousScanActor` per index DB when enabled in config.
  - A ractor factory (per DB) runs per-file processing workers; DB writes still go through the index DB writer actor (serialized).
- Startup + discovery:
  - On startup, supervisor enumerates DBs in `data_folder`, loads each config, and spawns per-DB actors when enabled.
  - Supervisor watches `<data_folder>/index` for FS changes to react to DB additions/removals and config edits.
  - The config update API notifies the supervisor directly on changes (fast-path).
- Pause/resume semantics (no job queue coupling, but reactive):
  - Continuous scan runs concurrently with data extraction jobs and file scans on other DBs.
  - It pauses when a `folder_rescan`/`folder_update` starts on the same DB.
  - Job runner pauses before starting a file scan job and resumes after completion (if still enabled).
- Epoch gating (write safety guarantee):
  - Each `ContinuousScanActor` tracks `epoch: u64`, `paused: bool`, and `paused_for_job`.
  - Every worker task is dispatched with the current epoch.
  - Before any DB write, the actor checks `paused == false` and `task_epoch == current_epoch`.
  - Pause increments `epoch` and sets `paused = true`, so stale results are dropped without writing.
  - This guarantees no file/folder writes after a file scan job starts, without blocking on in-flight tasks.
- File scan rows:
  - Each continuous run creates a new `file_scans` row (path sentinel `"<continuous>"`).
  - On pause/stop/config disable, close the row (`end_time = now`) and do not reuse it.
  - On resume, create a new row with a new `scan_id`.
  - On startup, any open continuous scan row is closed before creating a new one.
- Allowed operations:
  - Continuous scan can add new files/items and delete files it created in the current continuous session after verifying they are truly missing.
  - No content-update semantics: file content change == delete old file row + create new file row (new hash -> new item).
  - Items with no files are always deleted. If an item should survive, its file should not be deleted.
  - No mark-unavailable + sweep deletes; those remain full scan responsibilities.
- Deletion policy (conservative):
  - Never delete on fs event alone; always verify on disk.
  - Safe deletes:
    - Files created in the current continuous scan (`scan_id == current_scan_id`).
    - Duplicate files for an item (other file rows exist).
  - Otherwise, avoid destructive deletes and defer to full scan.
- Move handling:
  - Rename events (old_path -> new_path) are safe to apply because they do not delete items; treat them as a path update on the file row.
  - If a move appears as delete+create (no rename event), process it directly as delete+create.
- Cross-platform file watching:
  - Use `notify` with native backends (Windows/macOS/Linux).
- Optional polling mode when `[continuous_filescan].poll_interval_secs` is set uses the hierarchical directory-mtime poller: idle passes stat directories and enumerate only changed directories, rather than rescanning or hashing every file. It detects entry changes but may miss in-place content edits until the next full scan.
- Watcher overflow logs a warning (index_db + watched roots); no automatic recovery action.
- For unreliable shares (SMB/NFS), add an explicit config opt-in to use `notify::PollWatcher` with a configurable interval (e.g., `[continuous_filescan].poll_interval_secs`); default remains native watchers.
- When `[continuous_filescan].included_folders` is non-empty, watcher roots are limited to those paths; they must be within the global `included_folders` and not under `excluded_folders`, otherwise continuous scanning is disabled for that DB until fixed.
- No continuous-scan exclude list is implemented because notify backends do not support watcher-level excludes.
