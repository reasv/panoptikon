#!/usr/bin/env node
// Runs a whole scenario matrix through cdp-scroll-bench.mjs and prints one
// markdown table (plus the raw JSON with --json).
//
//   node run-matrix.mjs synthetic --port 9231 [--base http://127.0.0.1:8777]
//   node run-matrix.mjs stdtest   --port 9231 [--base http://127.0.0.1:6343]
//
// `synthetic` reproduces the baseline table in
// docs/grid-scroll-performance-implementation.md §1 against the local grid page
// (start server.mjs first, and run gen-images.mjs before that).
// `stdtest` runs the same shapes against the stdtest-locked gateway UI.
//
// Extra flags are forwarded to every run, so e.g. `--ms 40000` turns either
// matrix into the sustained-scroll degradation scenario.

import { spawnSync } from 'child_process';
import path from 'path';
import url from 'url';

const here = path.dirname(url.fileURLToPath(import.meta.url));
const bench = path.join(here, 'cdp-scroll-bench.mjs');

const argv = process.argv.slice(2);
const which = argv.find(a => !a.startsWith('--')) || 'synthetic';
const flags = {};
const passthrough = [];
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const k = a.slice(2);
  const next = argv[i + 1];
  const v = next && !next.startsWith('--') ? (i++, next) : true;
  if (k === 'port' || k === 'base' || k === 'json') flags[k] = v;
  else { passthrough.push(a); if (v !== true) passthrough.push(v); }
}
const port = flags.port || '9231';

const MATRICES = {
  // label, query string on the synthetic page, extra bench args
  synthetic: {
    base: flags.base || 'http://127.0.0.1:8777',
    rows: [
      ['originals            ', '/?mode=full&cols=5', ['--dir', 'down', '--reset']],
      ['originals (warm up)  ', '/?mode=full&cols=5', ['--dir', 'up', '--warm']],
      ['t4096 = today        ', '/?mode=t4096&cols=5', ['--dir', 'down', '--reset']],
      ['t4096 (warm up)      ', '/?mode=t4096&cols=5', ['--dir', 'up', '--warm']],
      ['grid-m 1024, 5 cols  ', '/?mode=t1024&cols=5', ['--dir', 'down', '--reset']],
      ['grid-m 1024 (warm up)', '/?mode=t1024&cols=5', ['--dir', 'up', '--warm']],
      ['grid-s 512, 10 cols  ', '/?mode=t512&cols=10&ch=304', ['--dir', 'down', '--reset']],
      ['grid-s 512 (warm up) ', '/?mode=t512&cols=10&ch=304', ['--dir', 'up', '--warm']],
    ],
  },
  stdtest: {
    base: flags.base || 'http://127.0.0.1:6343',
    rows: [
      ['scroll mode down     ', '/search?vm=scroll&page_size=50', ['--dir', 'down', '--reset', '--settle', '8000']],
      ['scroll mode warm up  ', '/search?vm=scroll&page_size=50', ['--dir', 'up', '--warm', '--settle', '8000']],
      ['scroll mode, no imgs ', '/search?vm=scroll&page_size=50', ['--dir', 'down', '--reset', '--settle', '8000', '--blockImages']],
      ['pages mode down      ', '/search?page_size=200', ['--dir', 'down', '--reset', '--settle', '8000']],
    ],
  },
};

const matrix = MATRICES[which];
if (!matrix) {
  console.error(`unknown matrix "${which}" (want ${Object.keys(MATRICES).join(' | ')})`);
  process.exit(1);
}

const out = [];
for (const [label, qs, extra] of matrix.rows) {
  const args = [bench, '--port', String(port), '--url', matrix.base + qs, ...extra, ...passthrough];
  process.stderr.write(`running ${label.trim()} ...\n`);
  const r = spawnSync(process.execPath, args, { encoding: 'utf8', maxBuffer: 1 << 28 });
  if (r.status !== 0 || !r.stdout.trim()) {
    process.stderr.write((r.stderr || '').trim() + '\n');
    out.push({ label, error: (r.stderr || 'no output').trim().split('\n')[0] });
    continue;
  }
  const j = JSON.parse(r.stdout);
  out.push({ label, info: j.info, result: j.result });
}

const n = (v) => (v === null || v === undefined ? '-' : String(v));
console.log(`\n| scenario | mean | p90 | p99 | max | >32ms | longtasks | mounted MP | heap Δ |`);
console.log(`|---|---|---|---|---|---|---|---|---|`);
for (const row of out) {
  if (row.error) { console.log(`| ${row.label} | ERROR: ${row.error} | | | | | | | |`); continue; }
  const r = row.result;
  console.log(`| ${row.label} | ${n(r.meanMs)}ms | ${n(r.p90)}ms | ${n(r.p99)}ms | ${n(r.maxMs)}ms | ` +
    `${n(r.framesOver32)} | ${n(r.longtaskCount)} / ${n(r.longtaskTotalMs)}ms | ${n(row.info.megapixelsMounted)} | ${n(r.heapDeltaMB)}MB |`);
}
console.log(`\nviewport ${out.find(r => r.info)?.info.vp || '?'} @ dpr ${out.find(r => r.info)?.info.dpr || '?'}`);
for (const row of out) {
  if (row.result) console.log(`  ${row.label} buckets p90: ${row.result.buckets.map(b => b.p90).join(' -> ')}`);
}
if (flags.json) {
  const fs = await import('fs');
  fs.writeFileSync(String(flags.json), JSON.stringify(out, null, 1));
  console.log(`\nraw -> ${flags.json}`);
}
