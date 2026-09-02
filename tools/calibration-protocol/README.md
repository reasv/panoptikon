# Calibration test protocol tooling

The plan this directory serves is `docs/batch-calibration-test-protocol.md`.
`codemap.md` beside this file is the code-level reference (file:line) the
executing agents work from: how the host and the worker measure memory,
the wire fields, the log lines, the API calls, the env vars, the fixture
impls, and the suspected weak points.

Tools (to be written in Phase 0, all Python on the managed venv
`python/.venv`, stdlib + `nvidia-ml-py` + `psutil` + Pillow + torch):

| File | Purpose |
|---|---|
| `vramrec.py` | Out-of-process NVML recorder: per-board and per-PID VRAM at 250 ms, `/proc/meminfo`, per-PID RSS, worker attribution via `/proc/<pid>/environ`. JSONL. |
| `hog.py` | External-pressure generator (GPU via torch, RAM via numpy) with schedules and an HTTP control endpoint. |
| `corpus.py` | Deterministic media corpus with per-item unit cost manifest. |
| `healthrec.py` | Polls `/api/inference/health` and `/api/jobs/queue` at 500 ms. JSONL. |
| `loadgen.py` | Concurrent `POST /api/inference/predict/<id>` driver for contention scenarios. |
| `ceiling_probe.py` | Ground-truth base/slope/OOM boundary per model, outside the orchestrator. |
| `analyze.py` | Joins recordings and prints the verdict table. |
| `runlog.md` | Per-scenario report template. |
| `compose/` | Copies of any docker compose files used for pressure; never the user's own files. |

Results go under `results/<run-id>/<scenario>/` (git-ignored).
