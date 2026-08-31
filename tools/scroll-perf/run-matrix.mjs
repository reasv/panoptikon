#!/usr/bin/env node
// Runs a whole scenario matrix through cdp-scroll-bench.mjs and prints one
// markdown table (plus the raw JSON with --json).
//
//   node run-matrix.mjs synthetic --port 9231 [--base http://127.0.0.1:8777]
//   node run-matrix.mjs stdtest   --port 9231 [--base http://127.0.0.1:6343]
//
// The matrix name is positional in any position (`--port 9231 synthetic` works
// too) or explicit as `--matrix <name>`; it defaults to `synthetic`.
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

// Parse flags (consuming their values) and collect true positionals in the same
// pass -- a flag VALUE like the 9231 in `--port 9231 synthetic` must never be
// mistaken for the matrix name.
const argv = process.argv.slice(2);
const flags = {};
const passthrough = [];
const positional = [];
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) { positional.push(a); continue; }
  const k = a.slice(2);
  const next = argv[i + 1];
  const v = next && !next.startsWith('--') ? (i++, next) : true;
  if (k === 'port' || k === 'base' || k === 'json' || k === 'matrix') flags[k] = v;
  else { passthrough.push(a); if (v !== true) passthrough.push(v); }
}
if (flags.json === true) {
  console.error('--json needs a filename, e.g. --json matrix-out.json');
  process.exit(1);
}
if (flags.matrix === true) {
  console.error('--matrix needs a name, e.g. --matrix synthetic');
  process.exit(1);
}
const which = (typeof flags.matrix === 'string' ? flags.matrix : positional[0]) || 'synthetic';
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
    // The LAST non-empty stderr line: the driver's failures print a multi-line
    // explanation whose first line is the least specific part of it.
    const lines = (r.stderr || '').split('\n').map(s => s.trim()).filter(Boolean);
    out.push({ label, error: lines.length ? lines[lines.length - 1] : 'no output' });
    continue;
  }
  const j = JSON.parse(r.stdout);
  out.push({ label, info: j.info, result: j.result });
}

// p50 and the frame count are printed because the frame-time FLOOR is the
// display refresh interval, not zero: what proves smoothness is flatness
// (p50 ≈ p90 ≈ p99) and a plausible frame count, never the absolute numbers.
// Windows dynamic refresh rate can move the panel between runs, and rows whose
// p50 differs were measured against different floors -- not comparable.
const n = (v) => (v === null || v === undefined ? '-' : String(v));
console.log(`\n| scenario | mean | p50 | p90 | p99 | max | frames | >32ms | longtasks | mounted MP | heap Δ |`);
console.log(`|---|---|---|---|---|---|---|---|---|---|---|`);
for (const row of out) {
  if (row.error) { console.log(`| ${row.label} | ERROR: ${row.error} | | | | | | | | | |`); continue; }
  const r = row.result;
  console.log(`| ${row.label} | ${n(r.meanMs)}ms | ${n(r.p50)}ms | ${n(r.p90)}ms | ${n(r.p99)}ms | ${n(r.maxMs)}ms | ` +
    `${n(r.frames)} | ${n(r.framesOver32)} | ${n(r.longtaskCount)} / ${n(r.longtaskTotalMs)}ms | ` +
    `${n(row.info.megapixelsMounted)} | ${n(r.heapDeltaMB)}MB |`);
}

// Different p50s across rows = a different refresh floor, so absolute p90/p99
// comparisons between them are meaningless. Take the modal p50 as the floor the
// matrix mostly ran at and name the rows that deviate by more than 25%.
const p50s = out.filter(r => r.result && r.result.p50 > 0).map(r => ({ label: r.label.trim(), p50: r.result.p50 }));
if (p50s.length > 1) {
  const counts = new Map();
  for (const r of p50s) counts.set(r.p50, (counts.get(r.p50) || 0) + 1);
  const modal = [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0] - b[0])[0][0];
  const off = p50s.filter(r => Math.abs(r.p50 - modal) / modal > 0.25);
  if (off.length) {
    console.log(`\nWARNING: p50 differs materially across rows (most rows sat at ${modal}ms):`);
    for (const r of off) console.log(`  ${r.label}: p50 ${r.p50}ms`);
    console.log('  The frame-time floor IS the display refresh interval, so these rows were measured');
    console.log('  against a different floor (dynamic refresh rate, or a genuinely non-flat run).');
    console.log('  Compare flatness (p50 ≈ p90 ≈ p99) within a row -- not absolute p90 across rows.');
  }
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
