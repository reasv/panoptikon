# scroll-perf — grid scroll performance harness

The acceptance instrument for the work in
`docs/grid-scroll-performance-implementation.md`. It measures what a grid
actually costs while scrolling: frame-time distribution, long tasks,
degradation over time, DOM churn, transferred bytes and JS heap growth.

Two targets:

- **the synthetic grid** (`server.mjs` + `grid.html` + `gen-images.mjs`) — a
  framework-free page that reproduces `ResultGrid`'s remount behaviour with a
  controlled image set. Isolates image decode cost from everything React does.
- **a real gateway UI** — the stdtest-locked gateway on **6343**, or a throwaway
  fixture gateway on an unused port. Never 6342, never a real DB.

Plain Node ≥ 20: built-ins plus global `fetch`/`WebSocket`. No `package.json`,
no dependencies. `ffmpeg`/`ffprobe` on PATH are needed only by `gen-images.mjs`.

## Files

| file | what it is |
|---|---|
| `cdp-scroll-bench.mjs` | the driver: one scenario, one JSON object on stdout |
| `run-matrix.mjs` | runs a whole scenario matrix, prints a markdown table (`mean p50 p90 p99 max frames …`, plus a warning when rows' p50s disagree) |
| `server.mjs` | static server for the synthetic page (default port 8777) |
| `grid.html` | the synthetic virtualized grid |
| `gen-images.mjs` | generates the synthetic image set into `imgtest/` |
| `.gitignore` | `imgtest/` and saved traces are build artifacts, never committed |

## Launching the instrumented browser

**TRAP — the window must be visible.** `requestAnimationFrame` is throttled to a
standstill when the browser window is minimized, on another virtual desktop, or
covered by another window. A minimized window additionally suspends layout, so
even `Runtime.evaluate` can block. Measurements taken in that state are not
slow — they are absent (single-digit frame counts over eight seconds).

The driver defends against this three ways: it un-minimizes and raises the
window before measuring, it refuses to run while `document.visibilityState` is
not `visible` (override with `--allowHidden`), and it warns after the fact if
the frame count implies under ~10 fps. It cannot detect a window that is merely
*covered*, which is why the launch flags below matter.

Dedicated instance, own profile, own debugging port (9231 here; other agents use
other ports — pick a free one):

```powershell
Start-Process "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" -ArgumentList `
  '--user-data-dir=C:\Users\<you>\AppData\Local\Temp\scrollperf-edge', `
  '--remote-debugging-port=9231', `
  '--disable-backgrounding-occluded-windows', `
  '--disable-renderer-backgrounding', `
  '--disable-background-timer-throttling', `
  '--disable-features=CalculateNativeWinOcclusion', `
  '--no-first-run','--no-default-browser-check', `
  '--window-position=0,0','--window-size=3072,1728', `
  'about:blank'
```

`--disable-features=CalculateNativeWinOcclusion` is the one that stops Chromium
from freezing rAF when another window covers this one — without it, a shared
desktop silently invalidates every run. Size the window to the display you are
measuring: cell size and therefore decoded megapixels per screenful follow the
viewport, so runs are only comparable at the same viewport (it is echoed as
`info.vp` in the output).

Close the instance when done:

```powershell
Get-CimInstance Win32_Process -Filter "Name='msedge.exe'" |
  Where-Object { $_.CommandLine -like '*scrollperf-edge*' } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```

Chromium works identically; only the executable path changes.

## Recipe A — the synthetic baseline table

```bash
node gen-images.mjs                       # once; ~2 min, writes imgtest/
node server.mjs --port 8777 &             # leave running
node run-matrix.mjs synthetic --port 9231
```

The matrix name is positional in any position (`run-matrix.mjs --port 9231
synthetic` works too), can be given as `--matrix synthetic`, and defaults to
`synthetic`. Any other flag is forwarded to every run.

That is the whole baseline table of the plan's §1: originals / today's 4096px
thumbs / grid-m 1024 / grid-s 512, each cold-down and warm-up.

Every cold row passes `--reset`, and `--reset` issues CDP
`Network.clearBrowserCache` before navigating. That is what keeps cold rows cold
in a **reused** browser: without it the second and later runs would be served
from the disk cache (`netKB` collapsing to ~0) and would quietly be warm runs
wearing a cold label. Cookies are untouched.

Single scenarios, for iterating:

```bash
node cdp-scroll-bench.mjs --port 9231 --url "http://127.0.0.1:8777/?mode=t1024&cols=5" --dir down --reset
node cdp-scroll-bench.mjs --port 9231 --url "http://127.0.0.1:8777/?mode=full&cols=5"  --dir up   --warm
```

`grid.html` query params: `mode=full|t4096|t1024|t512`, `cols`, `ch` (cell
height px), `rows`, `overscan`, `nocache=1` (defeat the HTTP cache for
cold-transfer runs).

### The image set

`gen-images.mjs` writes originals plus three tier ladders into `imgtest/`
(gitignored — regenerate, never commit):

| file | dims | bytes | what it stands for |
|---|---|---|---|
| `jpeg-12mp.jpg` | 4000×3000 | ~1.1 MB | ordinary large photo — served directly today |
| `jpeg-33mp.jpg` | 7000×4700 | ~1.9 MB | high-res scan — served directly today |
| `jpeg-100mp.jpg` | 12000×8300 | ~3.0 MB | the ≤5 MB-any-dimensions serve-directly hole |
| `png-16mp.png` | 4600×3500 | ~7.6 MB | **over** today's rule — decode-cost upper bound |
| `t4096-*.jpg` | long side ≤ 4096 | ~1.1–1.2 MB | what panoptikon stores **today** |
| `t1024-*.jpg` | short side 1024 | ~0.2 MB | proposed `grid-m` |
| `t512-*.jpg` | short side 512 | ~0.06 MB | proposed `grid-s` |

Read `mode=full` honestly: the three JPEGs are all within today's
serve-directly rule, but `png-16mp.png` is not — at 7.6 MB and 4600 px wide it
is over both the ≤5 MB byte cap and the 4096 px dimension bound, so today it
would be thumbnailed rather than served. It stays in the set as an **upper
bound on decode cost** (and because the recorded baselines were measured with
it); the image set is deliberately never regenerated or resized, so those
baselines stay comparable.

The PNG is also low-entropy — ~0.47 B/px, against 25–45 MB for a real
photographic 16 MP PNG — so its zlib-inflate cost is under-represented by
roughly 4–6×. The dimension-driven costs (unfilter, the 64 MB RGBA surface, GPU
upload, resample) are fully represented. Do not read the PNG row as a
byte-cost datapoint.

Verify a regeneration with `ffprobe -v error -select_streams v:0 -show_entries
stream=width,height -of csv=p=0 imgtest/<file>`.

TRAP baked into the generator: ffmpeg's `mandelbrot`/`testsrc` sources fail to
allocate much past ~4000px a side, so the entropy is produced once at 4000×3000
and the 33/100 MP variants are upscaled from it and re-noised. The noise is what
keeps a huge JPEG from compressing to nothing — but PNG is lossless, so the PNG
gets no noise at all (a noised 16 MP PNG lands near 40 MB, outside every
realistic serving rule).

## Recipe B — the stdtest matrix

The stdtest-locked gateway on 6343 must already be running. This is **read-only
use**: scrolling issues search GETs and nothing else. Never scan or mutate it,
never touch 6342 or a real DB, never launch the production gateway.

```bash
node run-matrix.mjs stdtest --port 9231
```

Four scenarios: scroll mode down, scroll mode warm-up, scroll mode with images
blocked (isolates JS cost), pages mode with a large page. Add `--ms 40000` to
turn any of them into the sustained-scroll degradation scenario.

Single scenario:

```bash
node cdp-scroll-bench.mjs --port 9231 \
  --url "http://127.0.0.1:6343/search?vm=scroll&page_size=50" \
  --dir down --reset --settle 8000
```

## Driver flags

```
--port <n>         CDP port of the instrumented browser (default 9231)
--url <url>        navigate before measuring (otherwise measures the open page)
--target <substr>  pick the page target whose URL contains this substring
--selector <css>   scroll viewport element (default: auto-detect)
--velocity <px/s>  scroll speed (default 4000)
--ms <ms>          measurement duration (default 8000)
--dir down|up      direction; 'up' pre-seeks to the end first
--settle <ms>      wait after navigation / pre-seek (default 3000)
--reset            clear the HTTP cache and scrollTop = 0 before measuring (cold run)
--warm             slow pre-scroll over the range, then measure (warm run; --dir up only)
--pulse            scroll 600ms of every 1100ms (start/stop, not continuous)
--blockImages      block image requests via CDP -- isolates JS cost
--blockPattern <g> override the blocked globs (default: gateway thumbnails + /img/)
--allowHidden      measure even if the window is hidden (results are junk)
--trace [file]     record a DevTools trace; with a filename, also save it
                   (e.g. --trace trace-out.json -- gitignored)
```

`--warm` pairs only with `--dir up`: the warming pre-scroll ends at the **end**
of the range, which is where an up-run starts. `--warm --dir down` is an error
rather than a silently mismatched run; for a warm down-run, run the same down
scenario twice without `--reset`.

The viewport auto-detector picks the first scrollable `div` taller than 100 000
px, falling back to the tallest scrollable `div`, falling back to the document
scroller. That finds panoptikon's results pane and the synthetic page's
`#scroller`. Use `--selector` for anything else.

## Reading the output

```jsonc
{
 "scenario": { "url": "...", "dir": "down", "velocity": 4000, "ms": 8000, ... },
 "info": {
  "vp": "3064x1643",          // CSS viewport -- runs are comparable only at equal vp
  "dpr": 1.25,
  "imgs": 35,                 // <img> elements mounted at measurement start
  "megapixelsMounted": 976,   // sum of naturalWidth*naturalHeight -- THE cost driver
  "imgSample": ["12000x8300", ...]  // what the cells actually loaded
 },
 "result": { "megapixelsMountedMid": 1240, ... }  // same sum, sampled mid-run
}
```

> **`megapixelsMounted` is a start-of-run snapshot; don't compare it across
> directions.** A `--reset` down-run starts at `scrollTop = 0`, where the row
> window is clamped at the top of the document and therefore mounts roughly half
> the rows a mid-document run does. An up-run starts pre-seeked into the middle
> of the document with a full window. `result.megapixelsMountedMid`, sampled at
> the measurement midpoint, is the fair cross-direction number; the start
> snapshot is kept unchanged for continuity with earlier runs.

**`meanMs` / `p50` / `p90` / `p99` / `maxMs`** — interval between consecutive
rAF callbacks. The first frame is dropped (it carries navigation cost).

> **The floor is the display refresh interval, not zero.** A perfectly smooth
> run sits at ~4.2 ms on a 240 Hz panel and ~16.7 ms on a 60 Hz one, and Windows
> dynamic refresh rate can move a machine between the two between runs. What
> proves smoothness is *flatness and cadence*: `p50 ≈ p90 ≈ p99 ≈ the observed
> floor`, `framesOver32` near zero, `longtaskCount` zero. Never compare an
> absolute p90 across runs whose `p50` differs — that is a refresh-rate change,
> not a regression. `meanMs` above `p90` means a few catastrophic frames, which
> is the signature to look for in image-heavy runs.

**`framesOver32` / `framesOver100`** — dropped-frame counts. `framesOver32` is
the practical "did the user see stutter" number.

**`longtaskCount` / `longtaskTotalMs` / `longtaskTop`** — main-thread blocks
>50 ms from the Long Tasks API. Zero long tasks is the tier-M/tier-S signature;
seconds of long tasks is the originals signature.

**`buckets`** — the run split into ~5 s slices, each with its own p90/max. This
is where **degradation over time** shows up: a run whose buckets climb
(`16.8 -> 33.3`) is accumulating something, independently of scroll depth. The
F5 exit criterion is expressed against these (last bucket p90 ≤ 1.25× first).

**`heapMB` / `heapDeltaMB`** — `performance.memory` before/after. Positive delta
on an images-blocked long run is the accumulation signal; negative just means a
GC landed inside the window.

**`domAdded` / `domRemoved`** — mutation counts, i.e. remount churn.

**`netReqs` / `netKB`** — *all* resource-timing entries created during the run
and their transfer size (images included). Zero KB with unchanged frame times is
the crucial fact about warm scrolling: the cache kills the transfer, not the
decode.

**`apiReqs` / `apiKB`** — the same two numbers filtered to URLs containing
`/api/`, i.e. the gateway's search/metadata traffic with the image bytes
excluded. The request/KB figures quoted in the earlier investigation session
were this `/api/`-filtered pair, not the unfiltered one — compare like with
like.

**`traceSummaryMs`** (with `--trace`) — top 30 trace event names by *self* time
in ms, nesting subtracted. `ImageDecodeTask` / `Decode Image` dominating means
the image tier is the wall; `FunctionCall` / `EventDispatch` dominating means
the JS is.

Saved traces are ~100 MB each and are gitignored.

## Scenario matrix (plan §1)

Reproduced by `run-matrix.mjs synthetic`, at 4000 px/s, 8 s, 4K maximized:

| image source | direction | expected shape |
|---|---|---|
| originals served directly | down | p99 in the hundreds of ms, seconds of long tasks |
| originals served directly | warm up | **worse than down** — cached bytes let every decode fire at once |
| 4096px stored thumbs (today) | down / warm up | p90 25–35 ms, long tasks present — today's thumbs are not enough |
| grid-m 1024, 5 cols | down / warm up | flat at the refresh floor, zero long tasks |
| grid-s 512, 10 cols | down / warm up | flat at the refresh floor, zero long tasks |

The two flat rows are the target the whole plan is aiming at; the point of the
harness is that any step can be checked against them in one command.
