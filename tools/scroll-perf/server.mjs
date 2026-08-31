#!/usr/bin/env node
// Static server for the synthetic grid page.
//
//   node server.mjs [--port 8777] [--host 127.0.0.1]
//
// Serves grid.html from this directory and the generated tier images from
// ./imgtest/ under /img/<name>. Images are sent with immutable cache headers so
// warm re-scroll scenarios measure decode cost with zero transfers -- the same
// condition the real gateway produces for content-addressed thumbnail URLs.

import http from 'http';
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
const port = Number(args.port || 8777);
const host = args.host || '127.0.0.1';

const dir = path.dirname(url.fileURLToPath(import.meta.url));
const imgDir = path.join(dir, 'imgtest');
const mime = { '.jpg': 'image/jpeg', '.png': 'image/png', '.html': 'text/html; charset=utf-8', '.mp4': 'video/mp4' };

// Only ever serves files whose name survives this whitelist, resolved inside a
// fixed base directory -- no traversal reaches outside the tool.
const safeName = (s) => s.replace(/[^a-zA-Z0-9._-]/g, '');

http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  let base = dir, name;
  if (u.pathname === '/' || u.pathname === '/grid.html') {
    name = 'grid.html';
  } else if (u.pathname.startsWith('/img/')) {
    base = imgDir;
    name = safeName(u.pathname.slice('/img/'.length));
  } else {
    name = safeName(u.pathname.slice(1));
  }
  const file = path.join(base, name);
  if (!name || !fs.existsSync(file) || !fs.statSync(file).isFile()) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('not found: ' + u.pathname + (base === imgDir ? '\nrun: node gen-images.mjs\n' : '\n'));
    return;
  }
  const ext = path.extname(file);
  res.writeHead(200, {
    'Content-Type': mime[ext] || 'application/octet-stream',
    'Content-Length': fs.statSync(file).size,
    'Cache-Control': ext === '.html'
      ? 'no-store'
      : 'public, max-age=31536000, immutable',
  });
  fs.createReadStream(file).pipe(res);
}).listen(port, host, () => {
  console.log(`scroll-perf synthetic grid on http://${host}:${port}/`);
  if (!fs.existsSync(imgDir)) console.log('NOTE: imgtest/ missing -- run `node gen-images.mjs` first');
});
