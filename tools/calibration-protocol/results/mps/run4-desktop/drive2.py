#!/usr/bin/env python3
import json, sys, time, urllib.request, urllib.parse, urllib.error, pathlib

BASE = "http://127.0.0.1:16342"
OUT = pathlib.Path("/Users/moot/projects/panoptikon-pr27/.mac/desktop")
DB = "cal"
CORPUS = "/Users/moot/projects/panoptikon-pr27/.mac/desktop/corpus"
EVENTS = []

def mark(name, **kw):
    e = {"t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": name}; e.update(kw)
    EVENTS.append(e); print(json.dumps(e), flush=True)
    (OUT / "events2.json").write_text(json.dumps(EVENTS, indent=2))

def req(url, method="GET", body=None, ctype=None, timeout=60):
    r = urllib.request.Request(url, method=method, data=body)
    if ctype: r.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()

def getj(url, **kw):
    s, b = req(url, **kw)
    if s != 200: raise RuntimeError(f"{url} -> {s}: {b[:400]!r}")
    return json.loads(b)

def save(name, url, **kw):
    s, b = req(url, **kw); (OUT / name).write_bytes(b); return s, b

def queue_len():
    q = getj(f"{BASE}/api/jobs/queue", timeout=15)
    return len(q.get("queue", q) if isinstance(q, dict) else q)

def wait_drain(cap):
    t0 = time.monotonic()
    while True:
        try:
            if queue_len() == 0: return "drained", time.monotonic() - t0
        except Exception as e: print("poll", e)
        if time.monotonic() - t0 > cap: return "cap_exceeded", time.monotonic() - t0
        time.sleep(2)

# html text items (.txt has no scannable extension; scan_html is the text path)
tdir = pathlib.Path(CORPUS) / "text"
for i in range(1, 6):
    (tdir / f"desktop-note-{i}.html").write_text(
        f"<html><head><title>Desktop note {i}</title></head><body><p>Desktop calibration "
        f"text item {i}. The quick brown fox jumps over the lazy dog. Panoptikon desktop "
        f"run4 text item {i} for the text extraction path.</p></body></html>", encoding="utf-8")

cfg = getj(f"{BASE}/api/jobs/config?index_db={DB}")
cfg["scan_html"] = True
cfg["included_folders"] = [CORPUS]
s, _ = save("config-put2.json", f"{BASE}/api/jobs/config?index_db={DB}", method="PUT",
            body=json.dumps(cfg).encode(), ctype="application/json")
mark("scan_html_enabled", status=s)
save("rescan2.json", f"{BASE}/api/jobs/folders/rescan?index_db={DB}", method="POST")
outcome, secs = wait_drain(900)
save("folders2.json", f"{BASE}/api/jobs/folders/history?index_db={DB}&page=1&page_size=5")
rows = json.loads((OUT / "folders2.json").read_bytes())
rows = rows if isinstance(rows, list) else rows.get("history")
mark("rescan2_done", outcome=outcome, seconds=round(secs, 1), row=rows[0] if rows else None)

for model, tag in (("tags/wd-vit-tagger-v3", "tags"), ("clip/apple_MobileCLIP-S1", "clip")):
    mark("job_start", model=model)
    s, b = save(f"extraction-{tag}.json",
                f"{BASE}/api/jobs/data/extraction?index_db={DB}"
                f"&inference_ids={urllib.parse.quote(model)}", method="POST")
    if s != 200:
        mark("job_post_failed", model=model, status=s, body=b[:400].decode("utf-8", "replace"))
        continue
    outcome, secs = wait_drain(5400)
    save(f"jobs-{tag}.json", f"{BASE}/api/jobs/data/history?index_db={DB}&page=1&page_size=50")
    save(f"failures-{tag}.json", f"{BASE}/api/jobs/data/failures?index_db={DB}")
    save(f"health-{tag}.json", f"{BASE}/api/inference/health")
    hist = json.loads((OUT / f"jobs-{tag}.json").read_bytes())
    hrows = hist if isinstance(hist, list) else hist.get("history", hist)
    mark("job_end", model=model, outcome=outcome, seconds=round(secs, 1),
         latest=hrows[0] if hrows else None)

save("metadata-after.json", f"{BASE}/api/inference/metadata")
save("health-end.json", f"{BASE}/api/inference/health")

s, b = save("search-pql.json", f"{BASE}/api/search/pql?index_db={DB}", method="POST",
            body=json.dumps({"page_size": 5}).encode(), ctype="application/json")
res = json.loads(b) if s == 200 else None
mark("search_all", status=s, count=(res or {}).get("count"),
     first=((res or {}).get("results") or [{}])[0].get("path"))
sha = ((res or {}).get("results") or [{}])[0].get("sha256")
if sha:
    st, tb = save("thumb.bin", f"{BASE}/api/items/item/thumbnail?id={sha}&id_type=sha256&index_db={DB}")
    mark("thumbnail", status=st, bytes=len(tb))

# a semantic CLIP search: text query against the clip embeddings
sem = {"page_size": 5, "query": {"semantic": {"clip": {
    "query": "a colourful abstract pattern", "model": "clip/apple_MobileCLIP-S1"}}}}
s, b = save("search-semantic.json", f"{BASE}/api/search/pql?index_db={DB}", method="POST",
            body=json.dumps(sem).encode(), ctype="application/json")
mark("search_semantic", status=s, body=b[:300].decode("utf-8", "replace"))

# settings: per-model max batch size left on auto
cfg2 = getj(f"{BASE}/api/jobs/config?index_db={DB}")
(OUT / "config-final.json").write_text(json.dumps(cfg2, indent=2))
mark("settings_final", job_settings=cfg2.get("job_settings"), cron_jobs=cfg2.get("cron_jobs"))
print("DONE")
