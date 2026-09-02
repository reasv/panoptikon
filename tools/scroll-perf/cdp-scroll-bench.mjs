#!/usr/bin/env node
// CDP scroll benchmark driver (grid scroll performance harness).
//
// Drives a constant-velocity rAF scroll inside a page's scroll viewport over a
// Chrome DevTools Protocol connection and reports frame-time statistics, long
// tasks, DOM churn, network volume and JS heap delta. Optionally records a
// DevTools trace and attributes self-time per event name.
//
// Requires nothing but Node >= 20 built-ins (global fetch + WebSocket).
//
// Usage:
//   node cdp-scroll-bench.mjs --port 9231 --url http://127.0.0.1:8777/?mode=t1024
//        [--velocity 4000] [--ms 8000] [--dir down|up] [--settle 3000]
//        [--reset] [--warm] [--pulse] [--blockImages] [--blockPattern <glob>]
//        [--selector '#scroller'] [--target <url-substring>]
//        [--trace trace-out.json]
//
// Prints one JSON object to stdout. See README.md for how to read it.
//
// TRAP: requestAnimationFrame is throttled to a standstill when the browser
// window is occluded, minimized or in a hidden tab. The measured window must be
// visible on screen and the browser launched with background throttling off --
// see README.md "Launching the instrumented browser".

const args = {};
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a === '-h') { args.help = true; continue; }
  if (a.startsWith('--')) {
    const k = a.slice(2);
    const next = process.argv[i + 1];
    if (next && !next.startsWith('--')) { args[k] = next; i++; } else args[k] = true;
  }
}

if (args.help || args.h) {
  console.log(`cdp-scroll-bench.mjs -- constant-velocity scroll benchmark over CDP

  --port <n>           CDP port of the instrumented browser (default 9231)
  --url <url>          navigate before measuring (otherwise measures the open page)
  --target <substr>    pick the page target whose URL contains this substring
  --selector <css>     scroll viewport element (default: auto-detect)
  --velocity <px/s>    scroll speed (default 4000)
  --ms <ms>            measurement duration (default 8000)
  --dir down|up        scroll direction (default down; 'up' pre-seeks to the end)
  --settle <ms>        wait after navigation / pre-seek (default 3000)
  --reset              clear the HTTP cache and scrollTop = 0 before measuring
  --warm               slow pre-scroll over the range first (warm caches), then measure (--dir up only)
  --pulse              scroll 600ms of every 1100ms instead of continuously
  --blockImages        block image requests via CDP (isolates JS cost)
  --blockPattern <g>   override the blocked URL patterns (comma-separated globs)
  --rewrite <a>::<b>   rewrite substring <a> to <b> in every request URL
                       (comma-separated for several; e.g.
                       --rewrite size=grid-xs::size=grid-s serves the OLD tier)
  --dpr <n>            emulate devicePixelRatio <n> over the SAME physical pixel
                       area (CSS viewport is rescaled by the dpr ratio)
  --allowHidden        measure even if the window is hidden (results are junk)
  --trace [file]       record a DevTools trace; with a filename, also save it
                       (e.g. --trace trace-out.json -- gitignored)`);
  process.exit(0);
}

const port = args.port || '9231';
const velocity = Number(args.velocity || 4000);
const ms = Number(args.ms || 8000);
const dir = args.dir === 'up' ? 'up' : 'down';
const settle = Number(args.settle || 3000);
const selector = typeof args.selector === 'string' ? args.selector : null;

const DEFAULT_BLOCK = ['*/api/items/item/thumbnail*', '*/img/*'];
const blockPatterns = typeof args.blockPattern === 'string'
  ? args.blockPattern.split(',').map(s => s.trim()).filter(Boolean)
  : DEFAULT_BLOCK;

function fail(msg) { console.error(msg); process.exit(1); }

// --rewrite: counterfactual serving. A scenario like "what did this grid cost
// on the PREVIOUS thumbnail tier" needs the page unchanged and only the bytes
// it is answered with swapped, which is a URL substring rewrite at the network
// layer -- not a page edit and not a server change.
//
// The value is `<from>::<to>`. The separator is '::' and NOT '=' because the
// substrings this flag exists to swap are themselves query parameters:
// `size=grid-xs=size=grid-s` has four '=' and no rule over them recovers the
// intended split. ('=' is still accepted for a spec containing exactly one, so
// `/img/::/thumb/`-shaped and `a=b`-shaped both work.) Comma-separates several
// rewrites; each is applied in order to every intercepted URL.
const rewrites = (typeof args.rewrite === 'string' ? args.rewrite.split(',') : [])
  .map(s => s.trim()).filter(Boolean)
  .map(spec => {
    let i = spec.indexOf('::'), width = 2;
    if (i < 0) {
      if ((spec.match(/=/g) || []).length !== 1) {
        fail(`--rewrite wants <from>::<to>, got "${spec}"\n` +
          "'=' only works when the spec contains exactly one '='; a swap between two\n" +
          'query parameters must use "::" ' +
          '(e.g. --rewrite size=grid-xs::size=grid-s).');
      }
      i = spec.indexOf('='); width = 1;
    }
    if (i <= 0 || i + width >= spec.length) fail(`--rewrite wants <from>::<to>, got "${spec}"`);
    return { from: spec.slice(0, i), to: spec.slice(i + width) };
  });
const dprOverride = args.dpr != null && args.dpr !== true ? Number(args.dpr) : null;
if (dprOverride != null && !(dprOverride > 0)) fail(`--dpr wants a positive number, got "${args.dpr}"`);

// The pre-scroll leaves the viewport at the END of the range, which is the
// start of an up-scroll -- there is no warm variant of a down-scroll.
if (args.warm && args.dir !== 'up') {
  fail('--warm only pairs with --dir up: the warming pre-scroll ends at the END of the range,\n' +
    'which is exactly where an up-run starts.\n' +
    'For a warm down-run, run the same down scenario twice without --reset.');
}

let targets;
try {
  targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
} catch (e) {
  fail(`cannot reach CDP on 127.0.0.1:${port} (${e.message}). Is the instrumented browser running?`);
}

const pages = targets.filter(t => t.type === 'page' && !t.url.startsWith('devtools://'));
if (!pages.length) fail('no page target on this CDP endpoint');

// Prefer an explicit --target substring, then a page already on --url's origin,
// then the first page target (typically the about:blank we will navigate).
let originHint = null;
if (typeof args.url === 'string') { try { originHint = new URL(args.url).host; } catch { /* ignore */ } }
const page =
  (typeof args.target === 'string' && pages.find(t => t.url.includes(args.target))) ||
  (originHint && pages.find(t => t.url.includes(originHint))) ||
  pages[0];

const ws = new WebSocket(page.webSocketDebuggerUrl);
await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });

let msgId = 0;
const pending = new Map();
const eventHandlers = new Map();
ws.onmessage = (ev) => {
  const m = JSON.parse(ev.data);
  if (m.id && pending.has(m.id)) {
    const { res, rej } = pending.get(m.id);
    pending.delete(m.id);
    if (m.error) rej(new Error(m.error.message)); else res(m.result);
  } else if (m.method && eventHandlers.has(m.method)) {
    eventHandlers.get(m.method)(m.params);
  }
};
function send(method, params = {}) {
  return new Promise((res, rej) => {
    const id = ++msgId;
    pending.set(id, { res, rej });
    ws.send(JSON.stringify({ id, method, params }));
  });
}
async function evalJs(expr, timeoutMs = 120000) {
  const r = await Promise.race([
    send('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true }),
    new Promise((_, rej) => setTimeout(() => rej(new Error('eval timeout')), timeoutMs)),
  ]);
  if (r.exceptionDetails) throw new Error('page exception: ' + JSON.stringify(r.exceptionDetails).slice(0, 800));
  return r.result.value;
}
const sleep = (t) => new Promise(r => setTimeout(r, t));

await send('Page.enable');
await send('Network.enable');
await send('Performance.enable').catch(() => {});
// Renderer-level accumulation sentinel. The F3 investigation attributed the
// sustained-scroll degradation to isolated Blink Documents leaking (one per
// unique SVG blur placeholder); Documents is the cheap regression tell, and
// JSEventListeners/Nodes classify other leak shapes.
async function rendererMetrics() {
  try {
    const { metrics } = await send('Performance.getMetrics');
    const get = (n) => metrics.find(m => m.name === n)?.value;
    return { documents: get('Documents'), jsEventListeners: get('JSEventListeners'), nodes: get('Nodes') };
  } catch { return null; }
}
// Un-minimize and raise the window/tab: rAF is throttled to a standstill (and
// layout is suspended) while the window is minimized or occluded.
async function raiseWindow() {
  try {
    const { windowId, bounds } = await send('Browser.getWindowForTarget', { targetId: page.id });
    if (bounds.windowState === 'minimized') {
      await send('Browser.setWindowBounds', { windowId, bounds: { windowState: 'normal' } });
    }
  } catch { /* not a browser exposing Browser.* -- the visibility check still guards */ }
  await send('Page.bringToFront').catch(() => {});
}
await raiseWindow();
if (args.blockImages) {
  await send('Network.setBlockedURLs', { urls: blockPatterns });
}

// URL rewriting. Fetch.enable pauses every request matching a pattern, so the
// patterns are narrowed to the `from` substrings rather than '*': a bench that
// round-trips every request through the driver measures the driver.
let rewriteCount = 0;
if (rewrites.length) {
  eventHandlers.set('Fetch.requestPaused', async (p) => {
    let url = p.request.url;
    for (const r of rewrites) if (url.includes(r.from)) url = url.split(r.from).join(r.to);
    const changed = url !== p.request.url;
    if (changed) rewriteCount++;
    await send('Fetch.continueRequest', changed
      ? { requestId: p.requestId, url }
      : { requestId: p.requestId }).catch(() => {});
  });
  await send('Fetch.enable', {
    patterns: rewrites.map(r => ({ urlPattern: `*${r.from}*`, requestStage: 'Request' })),
  });
}

// DPR emulation over the SAME physical pixel area: the CSS viewport is scaled
// by the dpr ratio so the run paints the same number of device pixels, which
// is what makes an emulated row comparable to the native one. Without the
// rescale, a dpr change would silently also change how much grid is on screen.
let dprEmulation = null;
if (dprOverride != null) {
  const cur = await evalJs(`({w: innerWidth, h: innerHeight, dpr: devicePixelRatio})`, 20000);
  const width = Math.round(cur.w * cur.dpr / dprOverride);
  const height = Math.round(cur.h * cur.dpr / dprOverride);
  await send('Emulation.setDeviceMetricsOverride', {
    width, height, deviceScaleFactor: dprOverride, mobile: false,
  });
  dprEmulation = { nativeDpr: cur.dpr, dpr: dprOverride, cssViewport: `${width}x${height}`,
    devicePixels: `${Math.round(cur.w * cur.dpr)}x${Math.round(cur.h * cur.dpr)}` };
}

// A "cold" run is only cold if the HTTP cache is empty. In a reused browser the
// second run onwards would otherwise serve every image from the disk cache,
// silently turning cold rows warm (netKB collapses to ~0). Done before the
// navigation so the first screenful is cold too. Cookies are irrelevant to this
// harness, so Network.clearBrowserCookies is deliberately not issued.
if (args.reset) {
  await send('Network.clearBrowserCache').catch(e => {
    console.error(`WARNING: Network.clearBrowserCache failed (${e.message}) -- "cold" rows may be cache-warm.`);
  });
}

if (typeof args.url === 'string') {
  const loaded = new Promise(res => { eventHandlers.set('Page.loadEventFired', res); });
  await send('Page.navigate', { url: args.url });
  await Promise.race([loaded, sleep(15000)]);
  await sleep(settle);
}

// Fail fast rather than hang in a throttled rAF loop for a minute. On a shared
// desktop the window can be minimized between runs, so retry the raise a few
// times before giving up. Do this BEFORE any layout-forcing eval: a minimized
// window suspends layout and Runtime.evaluate can block indefinitely.
// A minimized window can make even this probe hang, so a timeout or a failed
// eval is itself evidence of a bad state: report it as visibility "unknown" and
// route through the same friendly failure instead of an unhandled rejection.
async function probeVisibility() {
  try {
    return await evalJs(`document.visibilityState`, 15000);
  } catch {
    return 'unknown';
  }
}
let visibility = await probeVisibility();
if (!args.allowHidden) {
  for (let i = 0; i < 5 && visibility !== 'visible'; i++) {
    await raiseWindow();
    await sleep(600);
    visibility = await probeVisibility();
  }
}
if (visibility !== 'visible' && !args.allowHidden) {
  fail(`document.visibilityState is "${visibility}": the measured window is hidden, minimized or occluded.\n` +
    'requestAnimationFrame is throttled to a standstill in that state, so frame times would be meaningless.\n' +
    'Raise the browser window on screen and re-run (see README.md), or pass --allowHidden to measure anyway.');
}

// Install the in-page harness. The viewport is either the --selector element or
// auto-detected: the first tall (>1e5px) scrollable div, falling back to the
// largest scrollable div, falling back to the document scroller.
await evalJs(`
window.__viewport = (() => {
  const explicit = ${JSON.stringify(selector)};
  if (explicit) {
    const el = document.querySelector(explicit);
    if (!el) throw new Error('no element matches selector ' + explicit);
    return el;
  }
  const scrollables = [...document.querySelectorAll('div')].filter(
    el => ['auto','scroll'].includes(getComputedStyle(el).overflowY) && el.scrollHeight > el.clientHeight + 1);
  return scrollables.find(el => el.scrollHeight > 1e5)
      || scrollables.sort((a,b) => b.scrollHeight - a.scrollHeight)[0]
      || document.scrollingElement;
})();
// The resource-timing buffer holds 250 entries by DEFAULT, and a dense grid
// blows through that during the settle alone -- after which netReqs/netKB read
// ZERO for the measured window and a transfer-heavy run looks like a cached
// one. Raise it before the run so the window's own entries are recorded.
try { performance.setResourceTimingBufferSize(50000); } catch { /* older engines */ }
window.__measure = async function(velocity, ms){
  const el = window.__viewport;
  const frames=[]; const longtasks=[]; let added=0, removed=0;
  const po = new PerformanceObserver(l=>{ for(const e of l.getEntries()) longtasks.push(Math.round(e.duration)); });
  po.observe({type:'longtask'});
  const mo = new MutationObserver(muts=>{ for(const m of muts){ added+=m.addedNodes.length; removed+=m.removedNodes.length; } });
  mo.observe(document.body,{childList:true,subtree:true});
  const res0 = performance.getEntriesByType('resource').length;
  const heap0 = performance.memory ? performance.memory.usedJSHeapSize : 0;
  const scrollTop0 = Math.round(el.scrollTop);
  // Mid-run mounted-megapixels sample. At scrollTop=0 the row window is clamped
  // at the top, so the start-of-run snapshot in info.megapixelsMounted under-
  // counts a cold down-run relative to a mid-document up-run.
  const mp = () => Math.round([...document.querySelectorAll('img')].reduce((a,i)=>a+i.naturalWidth*i.naturalHeight,0)/1e6);
  let megapixelsMountedMid = null;
  const midTimer = setTimeout(()=>{ megapixelsMountedMid = mp(); }, Math.round(ms/2));
  // User-timing marks delimiting the MEASURED window. trace-summary.mjs clips
  // to them, so a --trace run's category totals exclude the navigation, the
  // settle and the pre-seek that the raw trace also contains.
  performance.mark('scrollbench-start');
  const t0 = performance.now(); let lastT = t0; let lastFrame = t0;
  const pulse = ${args.pulse ? 'true' : 'false'};
  await new Promise(resolve=>{
    function step(t){
      frames.push(t-lastFrame); lastFrame=t;
      const dt=Math.min(t-lastT, 100); lastT=t;
      const phase = (t-t0) % 1100;
      const active = !pulse || phase < 600;
      if(active) el.scrollTop += velocity*dt/1000;
      if(t-t0<ms) requestAnimationFrame(step); else resolve();
    }
    requestAnimationFrame(step);
  });
  performance.mark('scrollbench-end');
  await new Promise(r=>setTimeout(r,300));
  clearTimeout(midTimer);
  po.disconnect(); mo.disconnect();
  const resEntries = performance.getEntriesByType('resource').slice(res0);
  const kb = Math.round(resEntries.reduce((a,r)=>a+r.transferSize,0)/1024);
  // netReqs/netKB are ALL resources; apiReqs/apiKB are the /api/ subset (what
  // the session-era numbers reported).
  const apiEntries = resEntries.filter(r=>r.name.includes('/api/'));
  const apiKb = Math.round(apiEntries.reduce((a,r)=>a+r.transferSize,0)/1024);
  frames.shift();
  const sorted=[...frames].sort((a,b)=>a-b);
  const q=p=>Math.round((sorted[Math.floor(p*sorted.length)]||0)*10)/10;
  return {
    scrollTopStart: scrollTop0,
    scrollTopEnd: Math.round(el.scrollTop),
    frames: frames.length, meanMs: Math.round(frames.reduce((a,b)=>a+b,0)/Math.max(1,frames.length)*10)/10,
    p50:q(.5), p90:q(.9), p99:q(.99), maxMs: Math.round(sorted[sorted.length-1]||0),
    framesOver32: frames.filter(f=>f>32).length, framesOver100: frames.filter(f=>f>100).length,
    longtaskCount: longtasks.length, longtaskTotalMs: longtasks.reduce((a,b)=>a+b,0), longtaskTop: [...longtasks].sort((a,b)=>b-a).slice(0,8),
    buckets: (()=>{ const B=[]; const per=Math.ceil(frames.length/Math.max(1,Math.round(ms/5000))); for(let i=0;i<frames.length;i+=per){ const s=[...frames.slice(i,i+per)].sort((a,b)=>a-b); B.push({p90:Math.round((s[Math.floor(.9*s.length)]||0)*10)/10, max:Math.round(s[s.length-1]||0), over32:s.filter(f=>f>32).length}); } return B; })(),
    domAdded: added, domRemoved: removed,
    megapixelsMountedMid,
    netReqs: resEntries.length, netKB: kb,
    apiReqs: apiEntries.length, apiKB: apiKb,
    heapMB: performance.memory ? Math.round(performance.memory.usedJSHeapSize/1048576) : null,
    heapDeltaMB: performance.memory ? Math.round((performance.memory.usedJSHeapSize-heap0)/1048576) : null
  };
};
'ok'`);

if (args.reset) { await evalJs(`window.__viewport.scrollTop = 0; 'ok'`); await sleep(1500); }

if (args.warm) {
  // Slow pre-scroll over the measurement range to warm caches/chunks. It ENDS
  // at the end of the range, which is exactly where the up-run starts -- hence
  // --warm requires --dir up. Makes the "warm re-scroll" scenario reproducible.
  await evalJs(`(async () => {
    const el = window.__viewport; el.scrollTop = 0;
    const target = ${velocity} * ${ms} / 1000;
    for (let y = 0; y <= target; y += 600) { el.scrollTop = y; await new Promise(r=>setTimeout(r,120)); }
    await new Promise(r=>setTimeout(r,3000));
    return 'warmed to ' + el.scrollTop;
  })()`, 300000);
}

if (dir === 'up') {
  const dist = velocity * ms / 1000;
  await evalJs(`window.__viewport.scrollTop = ${dist}; 'ok'`);
  await sleep(settle);
}

const info = await evalJs(`({vp: innerWidth+'x'+innerHeight, dpr: devicePixelRatio, sh: window.__viewport.scrollHeight, imgs: document.querySelectorAll('img').length, visibility: document.visibilityState,
  imgSample: [...document.querySelectorAll('img')].slice(0,6).map(i=>i.naturalWidth+'x'+i.naturalHeight),
  megapixelsMounted: Math.round([...document.querySelectorAll('img')].reduce((a,i)=>a+i.naturalWidth*i.naturalHeight,0)/1e6)})`);

let traceP = null;
if (args.trace) {
  const cats = ['devtools.timeline', 'disabled-by-default-devtools.timeline', 'disabled-by-default-devtools.timeline.frame', 'v8.execute', 'blink.user_timing'].join(',');
  traceP = new Promise(res => { eventHandlers.set('Tracing.tracingComplete', p => res(p)); });
  await send('Tracing.start', { categories: cats, transferMode: 'ReturnAsStream' });
}

const vel = dir === 'up' ? -velocity : velocity;
await raiseWindow();
const rmStart = await rendererMetrics();
const result = await evalJs(`window.__measure(${vel}, ${ms})`, ms + 60000);
const rmEnd = await rendererMetrics();
let renderer = null;
if (rmStart && rmEnd) {
  renderer = {
    documentsStart: rmStart.documents, documentsEnd: rmEnd.documents,
    documentsDelta: rmEnd.documents - rmStart.documents,
    jsEventListenersDelta: rmEnd.jsEventListeners - rmStart.jsEventListeners,
    nodesDelta: rmEnd.nodes - rmStart.nodes,
  };
  if (renderer.documentsDelta > 50) {
    console.error(`WARNING: renderer Documents grew ${rmStart.documents} -> ${rmEnd.documents} during the run -- ` +
      'isolated-Document churn (e.g. per-cell SVG placeholders). This is the F3 accumulation signature.');
  }
}

// An occluded (as opposed to minimized) window keeps visibilityState 'visible'
// while Chromium stops servicing rAF entirely, so the only tell is the frame
// count. Anything under ~10 fps average is not a measurement.
// Discriminate from a genuinely catastrophic run: throttled rAF yields few
// frames with ZERO long-task time, catastrophic decode yields few frames with
// seconds of it (the plan's originals rows are real ~6-frame measurements).
if (result.frames < ms / 100) {
  if (result.longtaskTotalMs < ms / 4) {
    console.error(`WARNING: only ${result.frames} frames in ${ms}ms with ${result.longtaskTotalMs}ms of long tasks -- requestAnimationFrame was throttled.\n` +
      'The measured window was almost certainly covered by another window. Bring it fully to the front\n' +
      '(and launch with --disable-features=CalculateNativeWinOcclusion) before trusting any of this.');
  } else {
    console.error(`note: only ${result.frames} frames in ${ms}ms, but ${result.longtaskTotalMs}ms of long tasks -- ` +
      'catastrophic frame times on a live window, not rAF throttling.');
  }
}

let traceSummary = null;
if (args.trace) {
  await send('Tracing.end');
  const complete = await traceP;
  const streamHandle = complete.stream;
  let data = '';
  while (true) {
    const chunk = await send('IO.read', { handle: streamHandle, size: 5_000_000 });
    data += chunk.base64Encoded ? Buffer.from(chunk.data, 'base64').toString('utf8') : chunk.data;
    if (chunk.eof) break;
  }
  await send('IO.close', { handle: streamHandle });
  const trace = JSON.parse(data);
  const events = trace.traceEvents || trace;
  // Self-time attribution per event name: subtract nested children's duration
  // from each complete ('X') event, summed per name across all threads.
  const byThread = new Map();
  for (const e of events) {
    if (e.ph !== 'X' || !e.dur) continue;
    const key = e.pid + ':' + e.tid;
    if (!byThread.has(key)) byThread.set(key, []);
    byThread.get(key).push(e);
  }
  const selfByName = new Map();
  for (const evs of byThread.values()) {
    evs.sort((a, b) => a.ts - b.ts || b.dur - a.dur);
    const stack = [];
    for (const e of evs) {
      while (stack.length && stack[stack.length - 1].ts + stack[stack.length - 1].dur <= e.ts) stack.pop();
      if (stack.length) {
        const parent = stack[stack.length - 1];
        parent.childDur = (parent.childDur || 0) + Math.min(e.dur, parent.ts + parent.dur - e.ts);
      }
      stack.push(e);
    }
    for (const e of evs) {
      const self = Math.max(0, e.dur - (e.childDur || 0));
      selfByName.set(e.name, (selfByName.get(e.name) || 0) + self);
    }
  }
  traceSummary = [...selfByName.entries()]
    .map(([name, us]) => [name, Math.round(us / 1000)])
    .filter(([, ms2]) => ms2 >= 5)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 30);
  if (typeof args.trace === 'string') {
    const fs = await import('fs');
    fs.writeFileSync(args.trace, data);
  }
}

// A rewrite that matched NOTHING is the failure mode this flag invites -- a
// mistyped substring produces a run that looks perfectly normal and silently
// measures the unmodified page. Say so rather than letting it pass as a result.
if (rewrites.length && rewriteCount === 0) {
  console.error(`WARNING: --rewrite matched 0 requests. This run measured the page UNCHANGED.\n` +
    `Rules: ${rewrites.map(r => `"${r.from}" -> "${r.to}"`).join(', ')}\n` +
    'Check the substring against a real request URL (DevTools Network, or the page\'s\n' +
    'resource timings). Remember the separator is "::", not "=".');
}

// Emulation overrides SURVIVE this connection closing, so a --dpr run would
// leave every later run on the same browser at the emulated viewport -- and
// those runs report the native dpr, so nothing in their output says the
// viewport is wrong. Clear it explicitly.
if (dprOverride != null) await send('Emulation.clearDeviceMetricsOverride').catch(() => {});

console.log(JSON.stringify({
  scenario: { url: typeof args.url === 'string' ? args.url : page.url, dir, velocity, ms, blockImages: !!args.blockImages, warm: !!args.warm, pulse: !!args.pulse },
  info, result, renderer, traceSummaryMs: traceSummary,
  rewrite: rewrites.length ? { rules: rewrites, rewritten: rewriteCount } : null,
  dprEmulation,
}, null, 1));
ws.close();
process.exit(0);
