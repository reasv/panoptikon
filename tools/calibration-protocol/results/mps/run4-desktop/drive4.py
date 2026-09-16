#!/usr/bin/env python3
import json, time, urllib.request, urllib.error, pathlib
BASE="http://127.0.0.1:16342"; OUT=pathlib.Path("/Users/moot/projects/panoptikon-pr27/.mac/desktop")
DB="cal2"; CORPUS="/Users/moot/projects/panoptikon-pr27/.mac/desktop/corpus"; EV=[]
def mark(n,**k):
    e={"t":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"event":n}; e.update(k); EV.append(e)
    print(json.dumps(e),flush=True); (OUT/"events4.json").write_text(json.dumps(EV,indent=2))
def req(u,m="GET",b=None,c=None,t=180):
    r=urllib.request.Request(u,method=m,data=b)
    if c: r.add_header("Content-Type",c)
    try:
        with urllib.request.urlopen(r,timeout=t) as x: return x.status,x.read()
    except urllib.error.HTTPError as e: return e.code,e.read()
def getj(u,**k):
    s,b=req(u,**k)
    if s!=200: raise RuntimeError(f"{u} -> {s}: {b[:300]!r}")
    return json.loads(b)
def save(n,u,**k):
    s,b=req(u,**k); (OUT/n).write_bytes(b); return s,b
def qlen():
    q=getj(f"{BASE}/api/jobs/queue",t=15); return len(q["queue"])
def drain(cap):
    t0=time.monotonic()
    while True:
        try:
            if qlen()==0: return "drained",time.monotonic()-t0
        except Exception: pass
        if time.monotonic()-t0>cap: return "cap_exceeded",time.monotonic()-t0
        time.sleep(2)

save("db2create.json",f"{BASE}/api/db/create?new_index_db={DB}&new_user_data_db={DB}",m="POST")
cfg=getj(f"{BASE}/api/jobs/config?index_db={DB}")
cfg["scan_html"]=True; cfg["included_folders"]=[CORPUS]
cfg["job_settings"]=[{"group_name":"tags","inference_id":"tags/wd-vit-tagger-v3",
                      "default_batch_size":8,"default_threshold":None}]
s,_=save("config2-put.json",f"{BASE}/api/jobs/config?index_db={DB}",m="PUT",
         b=json.dumps(cfg).encode(),c="application/json")
mark("cal2_configured",status=s)
save("rescan-cal2.json",f"{BASE}/api/jobs/folders/rescan?index_db={DB}",m="POST")
o,sec=drain(900); mark("cal2_rescan",outcome=o,seconds=round(sec,1))
s,b=save("extraction-cal2.json",f"{BASE}/api/jobs/data/extraction?index_db={DB}"
         f"&inference_ids=tags%2Fwd-vit-tagger-v3",m="POST")
mark("cal2_job_posted",status=s,body=b[:300].decode("utf-8","replace"))
o,sec=drain(3600)
save("jobs-cal2.json",f"{BASE}/api/jobs/data/history?index_db={DB}&page=1&page_size=10")
save("failures-cal2.json",f"{BASE}/api/jobs/data/failures?index_db={DB}")
h=json.loads((OUT/"jobs-cal2.json").read_bytes())
mark("cal2_job_done",outcome=o,seconds=round(sec,1),latest=(h if isinstance(h,list) else h.get("history"))[0])
print("DONE")
