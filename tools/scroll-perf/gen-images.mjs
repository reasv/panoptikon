#!/usr/bin/env node
// Generates the synthetic image set for the scroll-perf grid into ./imgtest/.
//
//   node gen-images.mjs [--out <dir>] [--force] [--only <name,name>]
//
// Requires ffmpeg + ffprobe on PATH. Everything it writes is a build artifact
// (imgtest/ is gitignored) -- never commit the binaries.
//
// What it produces
//   Originals -- the worst case today's serve-directly policy allows through:
//     jpeg-12mp.jpg   4000x3000    ~1 MB
//     jpeg-33mp.jpg   7000x4700    <3 MB
//     jpeg-100mp.jpg  12000x8300   <3 MB   (the "2.9 MB 100 MP" hole)
//     png-16mp.png    4600x3500    ~7 MB
//   Tiers derived from them:
//     t4096-*  LONG side <= 4096   -- what panoptikon stores today
//     t1024-*  SHORT side 1024     -- proposed grid-m
//     t512-*   SHORT side 512      -- proposed grid-s
//   All tier files are JPEG (.jpg), including the one derived from the PNG.
//
// TRAP: ffmpeg's synthetic sources (mandelbrot, testsrc) fail to allocate much
// beyond ~4000px on a side. So the entropy is generated once at 4000x3000 and
// the 33/100 MP variants are produced by scaling that up and re-noising it --
// upscaled-plus-noise keeps the high-frequency content that stops JPEG from
// compressing a huge frame down to nothing, which is what makes the decode cost
// realistic.

import { execFileSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import url from 'url';

const args = {};
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a.startsWith('--')) {
    const next = process.argv[i + 1];
    if (next && !next.startsWith('--')) { args[a.slice(2)] = next; i++; } else args[a.slice(2)] = true;
  }
}

const here = path.dirname(url.fileURLToPath(import.meta.url));
const outDir = path.resolve(args.out ? String(args.out) : path.join(here, 'imgtest'));
const force = !!args.force;
const only = typeof args.only === 'string' ? new Set(args.only.split(',').map(s => s.trim())) : null;

fs.mkdirSync(outDir, { recursive: true });

function ff(argv, label) {
  try {
    execFileSync('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', ...argv], { stdio: ['ignore', 'ignore', 'pipe'] });
  } catch (e) {
    const err = e.stderr ? e.stderr.toString().trim() : e.message;
    throw new Error(`ffmpeg failed (${label}): ${err}`);
  }
}
function probe(file) {
  const out = execFileSync('ffprobe', ['-v', 'error', '-select_streams', 'v:0',
    '-show_entries', 'stream=width,height', '-of', 'csv=p=0', file]).toString().trim();
  const [w, h] = out.split(',').map(Number);
  return { w, h };
}
const size = (f) => fs.statSync(f).size;
const mb = (b) => (b / 1048576).toFixed(2) + ' MB';
const wanted = (name) => !only || only.has(name);
const even = (n) => Math.max(2, Math.round(n / 2) * 2);

// Encode to JPEG, stepping ffmpeg's qscale down until the file fits the budget.
// (mjpeg -q:v is 2..31, lower = better; ~3 lands near libjpeg q85.)
function encodeJpeg(inputArgs, filter, out, { qStart = 3, budget = Infinity } = {}) {
  for (let q = qStart; q <= 20; q++) {
    ff([...inputArgs, '-frames:v', '1', '-vf', filter, '-q:v', String(q), out], path.basename(out));
    if (size(out) <= budget) return q;
  }
  return null;
}

const BASE = path.join(outDir, '_base-12mp.png');
const MB = 1024 * 1024;
// `budget` keeps each original inside the byte band that matters for the
// serve-directly rule (a 100 MP file must still be small enough to slip through
// a <=5 MB byte cap); the encoder picks the best quality that fits. `noise` is
// tuned per size so the frame stays high-entropy without becoming
// incompressible.
const ORIGINALS = [
  { name: 'jpeg-12mp.jpg', w: 4000, h: 3000, noise: 14, budget: 1.2 * MB },
  { name: 'jpeg-33mp.jpg', w: 7000, h: 4700, noise: 14, budget: 2.2 * MB },
  { name: 'jpeg-100mp.jpg', w: 12000, h: 8300, noise: 14, budget: 3.0 * MB },
  // PNG is lossless: any added noise makes it incompressible (a noised 16 MP
  // PNG lands near 40 MB, outside every realistic serving rule). The plain
  // upscale lands near the ~7 MB the real-world sample had.
  { name: 'png-16mp.png', w: 4600, h: 3500, noise: 0, png: true },
];
const TIERS = [
  { prefix: 't4096', bound: 'long', px: 4096 },
  { prefix: 't1024', bound: 'short', px: 1024 },
  { prefix: 't512', bound: 'short', px: 512 },
];

// 1. One high-entropy 12 MP source (the largest mandelbrot reliably allocates).
if (force || !fs.existsSync(BASE)) {
  console.log('generating base 4000x3000 entropy source...');
  ff(['-f', 'lavfi', '-i', 'mandelbrot=size=4000x3000:rate=1', '-frames:v', '1', BASE], 'base');
}

// 2. Originals: scale the base to the target dimensions and re-noise.
for (const o of ORIGINALS) {
  const out = path.join(outDir, o.name);
  if (!wanted(o.name)) continue;
  if (!force && fs.existsSync(out)) { console.log(`skip  ${o.name} (exists)`); continue; }
  const filter = `scale=${o.w}:${o.h}:flags=bicubic`
    + (o.noise ? `,noise=alls=${o.noise}:allf=t+u` : '');
  if (o.png) {
    ff(['-i', BASE, '-frames:v', '1', '-vf', filter, '-compression_level', '9', out], o.name);
  } else {
    const q = encodeJpeg(['-i', BASE], filter, out, { qStart: 2, budget: o.budget });
    if (q === null) console.error(`WARNING: ${o.name} could not be squeezed under ${mb(o.budget)}`);
  }
  const d = probe(out);
  console.log(`write ${o.name.padEnd(16)} ${d.w}x${d.h}  ${mb(size(out))}`);
}

// 3. Tier renditions. Never upscale: a tier bound above the source dimension
//    yields the source dimension. A JPEG already within a long-side bound is
//    copied verbatim, which is exactly what the server does today.
for (const t of TIERS) {
  for (const o of ORIGINALS) {
    const src = path.join(outDir, o.name);
    if (!fs.existsSync(src)) continue;
    const stem = o.name.replace(/\.(jpg|png)$/, '');
    const outName = `${t.prefix}-${stem}.jpg`;
    if (!wanted(outName)) continue;
    const out = path.join(outDir, outName);
    if (!force && fs.existsSync(out)) { console.log(`skip  ${outName} (exists)`); continue; }

    const { w, h } = probe(src);
    const cur = t.bound === 'long' ? Math.max(w, h) : Math.min(w, h);
    const scale = Math.min(1, t.px / cur);
    if (scale === 1 && !o.png) {
      fs.copyFileSync(src, out);
      console.log(`copy  ${outName.padEnd(24)} ${w}x${h}  ${mb(size(out))}  (already within ${t.bound} <= ${t.px})`);
      continue;
    }
    const tw = even(w * scale), th = even(h * scale);
    encodeJpeg(['-i', src], `scale=${tw}:${th}:flags=lanczos`, out, { qStart: 3 });
    const d = probe(out);
    console.log(`write ${outName.padEnd(24)} ${d.w}x${d.h}  ${mb(size(out))}`);
  }
}

console.log(`\ndone -> ${outDir}`);
