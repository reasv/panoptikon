#!/usr/bin/env python3
import json, time, urllib.request, urllib.error, pathlib
BASE="http://127.0.0.1:16342"; OUT=pathlib.Path("/Users/moot/projects/panoptikon-pr27/.mac/desktop"); DB="cal"
EV=[]
def mark(n,**k):
    e={"t":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"event":n}; e.update(k); EV.append(e)
    print(json.dumps(e),flush=True); (OUT/"events3.json").write_text(json.dumps(EV,indent=2))
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

# --- search 1: tag search over what the tags job wrote --------------------
tags = getj(f"{BASE}/api/search/stats?index_db={DB}") if False else None
q={"page_size":5,"query":{"match_tags":{"tags":["1girl"],"match_any":True,
   "setters":["tags/wd-vit-tagger-v3"],"min_confidence":0.2}}}
s,b=save("search-tags.json",f"{BASE}/api/search/pql?index_db={DB}",m="POST",
         b=json.dumps(q).encode(),c="application/json")
r=json.loads(b) if s==200 else None
mark("search_tags",status=s,count=(r or {}).get("count"),
     body=None if s==200 else b[:300].decode("utf-8","replace"))

# --- search 2: semantic CLIP search --------------------------------------
q={"page_size":5,"query":{"image_embeddings":{"query":"a colourful abstract pattern",
   "model":"clip/apple_MobileCLIP-S1"}}}
s,b=save("search-semantic.json",f"{BASE}/api/search/pql?index_db={DB}",m="POST",
         b=json.dumps(q).encode(),c="application/json",t=600)
r=json.loads(b) if s==200 else None
mark("search_semantic",status=s,count=(r or {}).get("count"),
     first=((r or {}).get("results") or [{}])[0].get("path"),
     body=None if s==200 else b[:300].decode("utf-8","replace"))

# --- settings: an explicit per-model "auto" entry round-trips as auto -----
cfg=getj(f"{BASE}/api/jobs/config?index_db={DB}")
cfg["job_settings"]=[{"group_name":"tags","inference_id":"tags/wd-vit-tagger-v3",
                      "default_batch_size":None,"default_threshold":None},
                     {"group_name":"clip","inference_id":"clip/apple_MobileCLIP-S1",
                      "default_batch_size":None,"default_threshold":None}]
s,b=save("config-auto-put.json",f"{BASE}/api/jobs/config?index_db={DB}",m="PUT",
         b=json.dumps(cfg).encode(),c="application/json")
back=getj(f"{BASE}/api/jobs/config?index_db={DB}")
(OUT/"config-auto-get.json").write_text(json.dumps(back,indent=2))
mark("settings_auto_roundtrip",put_status=s,job_settings=back.get("job_settings"),
     preserved=back.get("job_settings")==cfg["job_settings"])

# a cap value round-trips too (then restored to auto)
cfg2=dict(back); cfg2["job_settings"]=[dict(back["job_settings"][0],default_batch_size=8)]
s,_=save("config-cap-put.json",f"{BASE}/api/jobs/config?index_db={DB}",m="PUT",
         b=json.dumps(cfg2).encode(),c="application/json")
capped=getj(f"{BASE}/api/jobs/config?index_db={DB}")
mark("settings_cap_roundtrip",put_status=s,job_settings=capped.get("job_settings"))
cfg3=dict(capped); cfg3["job_settings"]=back["job_settings"]
s,_=save("config-auto-restore.json",f"{BASE}/api/jobs/config?index_db={DB}",m="PUT",
         b=json.dumps(cfg3).encode(),c="application/json")
final=getj(f"{BASE}/api/jobs/config?index_db={DB}")
(OUT/"config-final.json").write_text(json.dumps(final,indent=2))
mark("settings_restored_auto",put_status=s,job_settings=final.get("job_settings"))
print("DONE")
