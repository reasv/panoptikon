#!/usr/bin/env python3
"""Desktop calibration leg driver: talks only to the running Desktop app's gateway."""
import json, sys, time, urllib.request, urllib.parse, urllib.error, pathlib

BASE = "http://127.0.0.1:16342"
OUT = pathlib.Path("/Users/moot/projects/panoptikon-pr27/.mac/desktop")
DB = "cal"
CORPUS = "/Users/moot/projects/panoptikon-pr27/.mac/desktop/corpus"
EVENTS = []

def mark(name, **kw):
    e = {"t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": name}
    e.update(kw)
    EVENTS.append(e)
    print(json.dumps(e), flush=True)

def req(url, method="GET", body=None, ctype=None, timeout=60, host=None):
    r = urllib.request.Request(url, method=method, data=body)
    if ctype:
        r.add_header("Content-Type", ctype)
    if host:
        r.add_header("Host", host)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()

def getj(url, **kw):
    s, b = req(url, **kw)
    if s != 200:
        raise RuntimeError(f"{url} -> {s}: {b[:400]!r}")
    return json.loads(b)

def save(name, url, method="GET", body=None, ctype=None, timeout=60):
    s, b = req(url, method=method, body=body, ctype=ctype, timeout=timeout)
    (OUT / name).write_bytes(b)
    return s, b

def queue_len():
    q = getj(f"{BASE}/api/jobs/queue", timeout=15)
    return len(q.get("queue", q) if isinstance(q, dict) else q)

def wait_drain(cap, tag):
    t0 = time.monotonic()
    while True:
        try:
            if queue_len() == 0:
                return "drained", time.monotonic() - t0
        except Exception as e:
            print("queue poll error", e)
        if time.monotonic() - t0 > cap:
            return "cap_exceeded", time.monotonic() - t0
        time.sleep(2)

# --- 2. health / device ---------------------------------------------------
save("health-t0.json", f"{BASE}/api/inference/health")
mark("health_t0")

# --- 5. policy: no 403 on the desktop profile ----------------------------
policy = {}
for host in ("127.0.0.1", "localhost"):
    for path in ("/api/client-config", "/api/inference/health", "/api/jobs/queue",
                 "/api/jobs/config", "/api/inference/metadata", "/api/db"):
        s, _ = req(f"http://127.0.0.1:16342{path}", host=host, timeout=20)
        policy[f"{host}{path}"] = s
(OUT / "policy-probe.json").write_text(json.dumps(policy, indent=2))
mark("policy_probe", forbidden=[k for k, v in policy.items() if v == 403], statuses=policy)

# --- 3. database + folder + rescan ---------------------------------------
save("dbcreate.json", f"{BASE}/api/db/create?new_index_db={DB}&new_user_data_db={DB}", method="POST")
cfg = getj(f"{BASE}/api/jobs/config?index_db={DB}")
(OUT / "config-before.json").write_text(json.dumps(cfg, indent=2))

# --- 4. settings round-trip: PUT the config back unchanged ---------------
s, b = save("config-roundtrip-put.json", f"{BASE}/api/jobs/config?index_db={DB}",
            method="PUT", body=json.dumps(cfg).encode(), ctype="application/json")
after = getj(f"{BASE}/api/jobs/config?index_db={DB}")
(OUT / "config-after-roundtrip.json").write_text(json.dumps(after, indent=2))
mark("settings_roundtrip", put_status=s, identical=(after == cfg),
     job_settings=after.get("job_settings"),
     cron_jobs=after.get("cron_jobs"))

cfg["included_folders"] = [CORPUS]
s, b = save("config-put.json", f"{BASE}/api/jobs/config?index_db={DB}",
            method="PUT", body=json.dumps(cfg).encode(), ctype="application/json")
mark("folder_added", status=s, folder=CORPUS)
save("rescan.json", f"{BASE}/api/jobs/folders/rescan?index_db={DB}", method="POST")
outcome, secs = wait_drain(900, "rescan")
save("folders.json", f"{BASE}/api/jobs/folders/history?index_db={DB}&page=1&page_size=5")
hist = json.loads((OUT / "folders.json").read_bytes())
rows = hist if isinstance(hist, list) else hist.get("history")
mark("rescan_done", outcome=outcome, seconds=round(secs, 1), row=rows[0] if rows else None)

# --- 3b. jobs -------------------------------------------------------------
for model, tag in (("wd-vit-tagger-v3", "tags"), ("apple_MobileCLIP-S1", "clip")):
    mark("job_start", model=model)
    s, b = save(f"extraction-{tag}.json",
                f"{BASE}/api/jobs/data/extraction?index_db={DB}"
                f"&inference_ids={urllib.parse.quote(model)}", method="POST")
    outcome, secs = wait_drain(3600, tag)
    save(f"jobs-{tag}.json", f"{BASE}/api/jobs/data/history?index_db={DB}&page=1&page_size=50")
    save(f"failures-{tag}.json", f"{BASE}/api/jobs/data/failures?index_db={DB}")
    save(f"health-{tag}.json", f"{BASE}/api/inference/health")
    mark("job_end", model=model, post_status=s, outcome=outcome, seconds=round(secs, 1))

save("metadata.json", f"{BASE}/api/inference/metadata")
save("health-end.json", f"{BASE}/api/inference/health")

# --- 3c. search -----------------------------------------------------------
pql = {"query": {"and_": [{"match": {"path": {"match": "img-1024"}}}]},
       "results": True, "count": True, "page_size": 10}
s, b = save("search-pql.json", f"{BASE}/api/search/pql?index_db={DB}",
            method="POST", body=json.dumps(pql).encode(), ctype="application/json")
mark("search_pql", status=s, body=json.loads(b) if s == 200 else b[:400].decode("utf-8", "replace"))

(OUT / "events.json").write_text(json.dumps(EVENTS, indent=2))
print("DONE")
