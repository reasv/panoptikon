# run4-proxy — the access policy behind a reverse proxy

master `7aa92b20` vs the batch-calibration branch `0d7f5671` (PR #27)

Branch under test: `claude/batch-calibration-coverage-db9ab9` @ `0d7f5671`
(PR #27). Baseline: `master` @ `7aa92b20`. Binaries:
`/home/admin/projects/panoptikon-wt/run4-upgrade/target/release/panoptikon`
(branch) and `/home/admin/projects/panoptikon-master/target/release/panoptikon`
(master), both `cargo build --release -p panoptikon` of those exact tips.
CPU only (`CUDA_VISIBLE_DEVICES=""`), local inference disabled, no GPU touched.

## 1. What changed and why this run exists

`docs/batch-calibration-run2-report.md` §6 row **P1** records the branch's fix:
`policy::resolve_effective_host` used to read only `header::HOST`, so every
HTTP/2 request — which carries its authority in `:authority` and sends no
`Host` at all (RFC 9113 §8.3.1) — resolved *hostless*, `select_policy`
declined a `hosts = [...]` policy, and the answer was 403 `no_policy`. The fix
makes the effective host

> trusted `Forwarded` / `X-Forwarded-Host` (only under
> `[server] trust_forwarded_headers`) **>** the request target's authority
> (h2 `:authority`, h1.1 absolute-form) **>** `Host` **>** none.

Only the loopback self-call case was ever measured. This run measures the
**reverse-proxy** case: nginx and Caddy in front of the gateway, with a
host-scoped `[policies.match] hosts` policy.

The deciding code, on the branch:

| what | branch | master |
|---|---|---|
| effective-host resolution | `panoptikon/src/policy.rs:499-515` | `policy.rs:472-488` |
| the new authority reader | `policy.rs:472-484` (`request_authority`) | — (did not exist) |
| the line that changed | `policy.rs:514` `request_authority(req.uri(), req.headers()).map(normalize_host)` | `policy.rs:487` `header_to_str(req.headers().get(header::HOST)).map(normalize_host)` |
| `userinfo@` strip in `normalize_host` | `policy.rs:540-546` | — (did not exist) |
| policy selection | `policy.rs:602-616` (`select_policy`), unchanged | `policy.rs:568-582` |
| 403 `no_policy` | `policy.rs:247-250` | same |
| 403 `ruleset_denied` | `policy.rs:288-296` | same |
| unit tests for the above | `policy.rs:1255-1292` (`effective_host_reads_authority_host_and_forwarded`) | — |

`config/server/*.toml` gained no new live lines for this: the only change is
comment text (`config/server/default.toml` `trust_forwarded_headers` gained a
cross-reference, and the Policies block gained the precedence paragraph). The
docs are `panoptikon/README.md` "Policy enforcement" and
`docs/inferio-transport.md` "Request authority (policy.rs)".

## 2. Method

### 2.1 Configs

Six gateways, identical TOML except for the two axes under test. The full
files are in `configs/` (`${RUN4_PROXY_ROOT}` stands for the run's scratch
root). Every one of them carries the same policy set:

```toml
[rulesets.allow_all]
allow_all = true

[rulesets.public_only]
allow = [
    { methods = ["GET"], path = "/openapi.json" },
    { methods = ["GET"], path_prefix = "/api/search/" },
]

# endpoint-scoped, listed first: the legacy [[server.endpoints]] listener
[[policies]]
name = "legacy_endpoint"
ruleset = "allow_all"
[policies.match]
endpoints = ["legacy_ui"]

# the host-scoped policy under test: allows one name, and only that name
[[policies]]
name = "allowed_host"
ruleset = "allow_all"
[policies.match]
hosts = ["panoptikon.example.com"]

# the control: hosts = [] matches any host, with a restricted ruleset so the
# two are distinguishable by status as well as by name
[[policies]]
name = "control_any"
ruleset = "public_only"
[policies.match]
hosts = []
endpoints = ["default", "legacy_ui"]
```

Note on the control: `config.rs:1470-1475` rejects a policy whose `hosts`
**and** `endpoints` are both empty ("must list at least one host or
endpoint"), so the `hosts = []` control is scoped to the two listener names.
That does not weaken it — both listeners are covered, so it is a catch-all in
every shape this run sends.

| gateway | binary | `trust_forwarded_headers` | `control_any` present | primary port | `legacy_ui` port |
|---|---|---|---|---|---|
| `m_trustfalse` | master | `false` | yes | 6842 | 6843 |
| `m_trusttrue`  | master | `true`  | yes | 6844 | 6845 |
| `m_strict`     | master | `false` | **no** | 6846 | 6847 |
| `b_trustfalse` | branch | `false` | yes | 6852 | 6853 |
| `b_trusttrue`  | branch | `true`  | yes | 6854 | 6855 |
| `b_strict`     | branch | `false` | **no** | 6856 | 6857 |

The "strict" pair drops the catch-all so that an unmatched host produces the
hard 403 `no_policy` (`policy.rs:247-250`) instead of silently falling through
— that is the shape P1 was about.

### 2.2 Probes

Three routes per shape:

- `GET /api/client-config` — **exempt** from ruleset enforcement
  (`policy.rs:280-281`), so it always answers 200 and its body's `policy`
  field names the policy that matched. This is the "which policy" readout.
- `GET /api/db` — a **policy-guarded route the UI uses** (the DB selector).
  `allow_all` permits it, `public_only` does not, so it reads 200 under
  `allowed_host` and 403 `ruleset_denied` under `control_any`.
- `GET /api/search/stats` — a **public** route, permitted by both rulesets.

Logging: `RUST_LOG="info,panoptikon::policy=debug"`, which turns on the
policy layer's own per-request line (`policy.rs:113-122`: `policy`,
`selected_by`, `endpoint`, `status`) and keeps the `warn` denial line
(`policy.rs:94-99`: `reason=no_policy` / `ruleset_denied`). Excerpts in
`raw/policy-log-excerpts.txt`.

### 2.3 Clients

`curl` 8.22.0 cannot send an HTTP/2 `:authority` and a literal `host` header
that disagree — `-H 'Host: …'` simply *replaces* `:authority`. So the h2c
shapes use `tools/h2req.py`, a ~60-line `h2`-library client that sets each
field independently. `tools/h2dump.py` and `tools/rawdump.py` are the
mirror-image servers used to record exactly what each proxy put on the wire.

### 2.4 Containers

`run4rp-nginx` (`nginx:1.29`, 1.29.8) and `run4rp-caddy` (`caddy:2`, v2.11.4),
both `--network host`, config bind-mounted read-only, both removed at the end
of the run. `~/docker` was not touched.

## 3. Results — direct requests

`allowed` = `panoptikon.example.com`, `denied` = `denied.example.net`.
A bolded number is a non-200. "Δ" marks a row where master and branch differ.

#### 3.1 Direct, catch-all config, `trust_forwarded_headers = false`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| D1 | h1.1 Host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| D2 | h1.1 Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |
| D3 | h1.0 no Host | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |
| D4 | h1.1 abs-form auth=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| D5 | h1.1 abs-form auth=allowed Host=denied | 200 `control_any` | 200 `allowed_host` | **403** | 200 | 200 | 200 | **yes** |
| D6 | h1.1 abs-form auth=denied Host=allowed | 200 `allowed_host` | 200 `control_any` | 200 | **403** | 200 | 200 | **yes** |
| D7 | h2c :authority=allowed | 200 `control_any` | 200 `allowed_host` | **403** | 200 | 200 | 200 | **yes** |
| D8 | h2c :authority=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |
| D9 | h2c :authority=allowed host=denied | 200 `control_any` | 200 `allowed_host` | **403** | 200 | 200 | 200 | **yes** |
| D10 | h2c :authority=denied host=allowed | 200 `allowed_host` | 200 `control_any` | 200 | **403** | 200 | 200 | **yes** |
| D11 | h2c no :authority host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| D12 | h2c no :authority no host | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |

Exact lines (`PORT` = 6842 master, 6852 branch; `ROUTE` = one of the three
above):

```
D1   curl -sS --http1.1 -H 'Host: panoptikon.example.com' http://127.0.0.1:PORT/ROUTE
D2   curl -sS --http1.1 -H 'Host: denied.example.net'     http://127.0.0.1:PORT/ROUTE
D3   curl -sS --http1.0 -H 'Host:'                        http://127.0.0.1:PORT/ROUTE
D4   curl -sS --http1.1 --proxy http://127.0.0.1:PORT      http://panoptikon.example.com/ROUTE
D5   curl -sS --http1.1 --proxy http://127.0.0.1:PORT -H 'Host: denied.example.net'     http://panoptikon.example.com/ROUTE
D6   curl -sS --http1.1 --proxy http://127.0.0.1:PORT -H 'Host: panoptikon.example.com' http://denied.example.net/ROUTE
D7   h2req.py 127.0.0.1 PORT /ROUTE --authority panoptikon.example.com:PORT
D8   h2req.py 127.0.0.1 PORT /ROUTE --authority denied.example.net:PORT
D9   h2req.py 127.0.0.1 PORT /ROUTE --authority panoptikon.example.com --host denied.example.net
D10  h2req.py 127.0.0.1 PORT /ROUTE --authority denied.example.net --host panoptikon.example.com
D11  h2req.py 127.0.0.1 PORT /ROUTE --host panoptikon.example.com
D12  h2req.py 127.0.0.1 PORT /ROUTE
```

`--proxy` is how curl is made to emit an HTTP/1.1 **absolute-form** request
target (`GET http://host/path HTTP/1.1`); the gateway is the "proxy". D4-D6
are that shape. `--http2-prior-knowledge` cannot express D9/D10 because curl
folds `-H 'Host: …'` into `:authority`, hence `h2req.py`.
#### 3.2 Direct, strict config (no catch-all policy), `trust_forwarded_headers = false`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| D1 | h1.1 Host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| D2 | h1.1 Host=denied | **403** | **403** | **403** | **403** | **403** | **403** | same |
| D3 | h1.0 no Host | **403** | **403** | **403** | **403** | **403** | **403** | same |
| D4 | h1.1 abs-form auth=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| D5 | h1.1 abs-form auth=allowed Host=denied | **403** | 200 `allowed_host` | **403** | 200 | **403** | 200 | **yes** |
| D6 | h1.1 abs-form auth=denied Host=allowed | 200 `allowed_host` | **403** | 200 | **403** | 200 | **403** | **yes** |
| D7 | h2c :authority=allowed | **403** | 200 `allowed_host` | **403** | 200 | **403** | 200 | **yes** |
| D8 | h2c :authority=denied | **403** | **403** | **403** | **403** | **403** | **403** | same |
| D9 | h2c :authority=allowed host=denied | **403** | 200 `allowed_host` | **403** | 200 | **403** | 200 | **yes** |
| D10 | h2c :authority=denied host=allowed | 200 `allowed_host` | **403** | 200 | **403** | 200 | **403** | **yes** |
| D11 | h2c no :authority host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| D12 | h2c no :authority no host | **403** | **403** | **403** | **403** | **403** | **403** | same |

Same curl lines, `PORT` = 6846 (master) / 6856 (branch). Here the divergences
are hard 403s rather than a different policy: D5/D7/D9 are **403 on master and
200 on branch**, D6/D10 are **200 on master and 403 on branch**.
#### 3.3 Forwarded headers, direct (`/api/client-config`, matched policy shown)

| id | request shape | master trust=false | branch trust=false | master trust=true | branch trust=true | Δ |
|---|---|---|---|---|---|---|
| F1 | h1.1 Host=denied XFH=allowed | 200 `control_any` | 200 `control_any` | 200 `allowed_host` | 200 `allowed_host` | same |
| F2 | h1.1 Host=allowed XFH=denied | 200 `allowed_host` | 200 `allowed_host` | 200 `control_any` | 200 `control_any` | same |
| F3 | h1.1 Host=denied Forwarded=host=allowed | 200 `control_any` | 200 `control_any` | 200 `allowed_host` | 200 `allowed_host` | same |
| F4 | h2c :authority=denied XFH=allowed | 200 `control_any` | 200 `control_any` | 200 `allowed_host` | 200 `allowed_host` | same |
| F5 | h2c :authority=allowed XFH=denied | 200 `control_any` | 200 `allowed_host` | 200 `control_any` | 200 `control_any` | **yes** |
| F6 | h2c no :authority XFH=allowed | 200 `control_any` | 200 `control_any` | 200 `allowed_host` | 200 `allowed_host` | same |

```
F1  curl -sS --http1.1 -H 'Host: denied.example.net' -H 'X-Forwarded-Host: panoptikon.example.com' http://127.0.0.1:PORT/api/client-config
F2  curl -sS --http1.1 -H 'Host: panoptikon.example.com' -H 'X-Forwarded-Host: denied.example.net' http://127.0.0.1:PORT/api/client-config
F3  curl -sS --http1.1 -H 'Host: denied.example.net' -H 'Forwarded: for=203.0.113.9;host=panoptikon.example.com;proto=https' http://127.0.0.1:PORT/api/client-config
F4  h2req.py 127.0.0.1 PORT /api/client-config --authority denied.example.net     --xfh panoptikon.example.com
F5  h2req.py 127.0.0.1 PORT /api/client-config --authority panoptikon.example.com --xfh denied.example.net
F6  h2req.py 127.0.0.1 PORT /api/client-config --xfh panoptikon.example.com
```

**With `trust_forwarded_headers = true` — the reverse-proxy deployment — master
and branch agree on all six shapes.** The forwarded header is read at
`policy.rs:500-512` and returns before the authority branch at `policy.rs:514`
is ever reached, so the changed line is unreachable whenever the front proxy
sets `X-Forwarded-Host` or `Forwarded` and the gateway trusts it. F5 is the
one trust=false divergence and it is D7 with an ignored header attached.
#### 3.4 Host/authority normalization (`/api/client-config`)

| id | request shape | master catch-all | branch catch-all | master strict | branch strict | Δ |
|---|---|---|---|---|---|---|
| U1 | h1.1 Host=evil.example.net@allowed | 200 `control_any` | 200 `allowed_host` | **403** | 200 `allowed_host` | **yes** |
| U2 | h1.1 Host=allowed@denied | 200 `control_any` | 200 `control_any` | **403** | **403** | same |
| U3 | h2c :authority=evil.example.net@allowed | 200 `control_any` | 200 `allowed_host` | **403** | 200 `allowed_host` | **yes** |
| U4 | h1.1 Host=PANOPTIKON.EXAMPLE.COM | 200 `allowed_host` | 200 `allowed_host` | 200 `allowed_host` | 200 `allowed_host` | same |
| U5 | h1.1 Host=allowed:8443 | 200 `allowed_host` | 200 `allowed_host` | 200 `allowed_host` | 200 `allowed_host` | same |

```
U1  curl -sS --http1.1 -H 'Host: evil.example.net@panoptikon.example.com' http://127.0.0.1:PORT/api/client-config
U2  curl -sS --http1.1 -H 'Host: panoptikon.example.com@denied.example.net' http://127.0.0.1:PORT/api/client-config
U3  h2req.py 127.0.0.1 PORT /api/client-config --authority evil.example.net@panoptikon.example.com
U4  curl -sS --http1.1 -H 'Host: PANOPTIKON.EXAMPLE.COM' http://127.0.0.1:PORT/api/client-config
U5  curl -sS --http1.1 -H 'Host: panoptikon.example.com:8443' http://127.0.0.1:PORT/api/client-config
```
#### 3.5 Direct to the `[[server.endpoints]]` legacy listener

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| L1 | legacy port h1.1 Host=allowed | 200 `legacy_endpoint` | 200 `legacy_endpoint` | 200 | 200 | 200 | 200 | same |
| L2 | legacy port h1.1 Host=denied | 200 `legacy_endpoint` | 200 `legacy_endpoint` | 200 | 200 | 200 | 200 | same |
| L3 | legacy port h2c :authority=denied | 200 `legacy_endpoint` | 200 `legacy_endpoint` | 200 | 200 | 200 | 200 | same |
| L4 | legacy port h2c no authority no host | 200 `legacy_endpoint` | 200 `legacy_endpoint` | 200 | 200 | 200 | 200 | same |

```
L1  curl -sS --http1.1 -H 'Host: panoptikon.example.com' http://127.0.0.1:LEGACY/ROUTE
L2  curl -sS --http1.1 -H 'Host: denied.example.net'     http://127.0.0.1:LEGACY/ROUTE
L3  h2req.py 127.0.0.1 LEGACY /ROUTE --authority denied.example.net
L4  h2req.py 127.0.0.1 LEGACY /ROUTE
```

`LEGACY` = 6843 (master) / 6853 (branch). All four shapes, all three routes,
both binaries: **200, policy `legacy_endpoint`**. Endpoint matching
(`policy.rs:236-239` for the `ListenerEndpoint` extension, `policy.rs:611-614`
for the comparison) is untouched by the change, and it outranks the host
question entirely because the endpoint-scoped policy is listed first.

## 4. Results — behind a reverse proxy

Client side is always plain HTTP/1.1 to the proxy:

```
curl -sS --http1.1 -H 'Host: panoptikon.example.com' http://127.0.0.1:PROXY_PORT/ROUTE
curl -sS --http1.1 -H 'Host: denied.example.net'     http://127.0.0.1:PROXY_PORT/ROUTE
```

`PROXY_PORT` per block below; the full nginx and Caddy files are
`configs/nginx.conf` and `configs/Caddyfile`.

### 4.0 What each proxy actually puts on the wire

Recorded by pointing each proxy block at a dumper instead of a gateway
(`tools/rawdump.py` for h1, `tools/h2dump.py` for h2c). Full captures:
`raw/nginx-upstream-headers.txt`, `raw/h2c-upstream-headers.txt`.

| proxy shape | upstream request line / pseudo-headers | `host` header |
|---|---|---|
| nginx h1.1, `proxy_set_header Host $host` | `GET /… HTTP/1.1` | `Host: panoptikon.example.com` |
| nginx h1.1, no Host override | `GET /… HTTP/1.1` | `Host: gw_master` (**the upstream block's name**), `X-Forwarded-Host: panoptikon.example.com` |
| nginx h2c, `proxy_set_header Host $host` | `:method :scheme :path` — **no `:authority` at all** | `host: panoptikon.example.com` (a regular h2 header) |
| nginx h2c, no Host override | `:authority: h2dump` (the upstream name) | none; `x-forwarded-host: panoptikon.example.com` |
| Caddy h2c, default | `:authority: panoptikon.example.com` | **none** |

That last row is the whole story of §4.8/§4.11: Caddy speaks h2c the way RFC
9113 §8.3.1 says to — `:authority` only, no `Host`. nginx, unusually, does the
opposite when told `proxy_set_header Host $host`: it emits `host` as an
ordinary header and omits `:authority` entirely, which is why master survives
that one.

### 4.0b Aside: nginx **can** do plain h2c upstream

nginx 1.29.8 accepts `proxy_http_version 2` on a plain `proxy_pass` and the
capture above shows the HTTP/2 connection preface (`PRI * HTTP/2.0`) going to
the upstream, so `grpc_pass` was not needed and Caddy was not the only option.
Caddy is included anyway because its default h2c framing (`:authority`, no
`Host`) is the *other* half of the matrix and is the shape that actually
breaks master.

#### 4.1 nginx, HTTP/1.1 upstream, `proxy_set_header Host $host`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| N1 | nginx h1.1 up, Host=$host; client Host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| N1 | nginx h1.1 up, Host=$host; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |

Ports: 6861 (master) / 6862 (branch). nginx block:

```nginx
upstream gw_master { server 127.0.0.1:6842; }
server { listen 127.0.0.1:6861;
    location / { proxy_pass http://gw_master;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Proto $scheme; } }
```
#### 4.2 nginx, HTTP/1.1 upstream, no Host override (+ `X-Forwarded-Host`), `trust_forwarded_headers = false`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| N2 | nginx h1.1 up, Host=upstream-name + XFH; client Host=allowed | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |
| N2 | nginx h1.1 up, Host=upstream-name + XFH; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6863 (master) / 6865 (branch). nginx block:

```nginx
server { listen 127.0.0.1:6863;
    location / { proxy_pass http://gw_master;
        proxy_http_version 1.1;
        proxy_set_header X-Forwarded-Host $host; } }
```

Both binaries see `Host: gw_master`, which matches no policy's `hosts`, and
both fall to `control_any` — the guarded route 403s for *every* client,
including the allowed name. This is the classic misconfiguration and it is
identical on master and branch.
#### 4.3 same nginx config, `trust_forwarded_headers = true`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| N2 | nginx h1.1 up, Host=upstream-name + XFH; client Host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| N2 | nginx h1.1 up, Host=upstream-name + XFH; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6864 (master) / 6866 (branch) — same nginx block, pointed at the
`trust_forwarded_headers = true` gateways. Identical on both binaries, and
correct: `X-Forwarded-Host` is consulted first (`policy.rs:506-511`).
#### 4.4 nginx in front of the `[[server.endpoints]]` legacy listener

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| N3 | nginx h1.1 up to legacy_ui port, Host=$host; client Host=allowed | 200 `legacy_endpoint` | 200 `legacy_endpoint` | 200 | 200 | 200 | 200 | same |
| N3 | nginx h1.1 up to legacy_ui port, Host=$host; client Host=denied | 200 `legacy_endpoint` | 200 `legacy_endpoint` | 200 | 200 | 200 | 200 | same |


Ports: 6867 (master) / 6868 (branch), upstream = the `legacy_ui` listener.
Identical on both binaries: policy `legacy_endpoint`, 200 on every route,
whatever the client's Host. **`[[server.endpoints]]` legacy-port behaviour is
unchanged behind the proxy.**
#### 4.5 nginx, h2c upstream (`proxy_http_version 2`), `proxy_set_header Host $host`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| N5a | nginx h2c up, Host=$host; client Host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| N5a | nginx h2c up, Host=$host; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6877 (master) / 6878 (branch). nginx block:

```nginx
server { listen 127.0.0.1:6877;
    location / { proxy_pass http://gw_master;
        proxy_http_version 2;
        proxy_set_header Host $host; } }
```

Identical on both binaries. nginx sends `host:` as a plain h2 header and *no*
`:authority` (§4.0), so master's `Host`-only read still finds the name and
the branch reaches the same answer through its `Host` fallback
(`policy.rs:483`).
#### 4.6 nginx, h2c upstream, no Host override (+ `X-Forwarded-Host`), trust=false

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| N5b | nginx h2c up, Host=upstream-name + XFH; client Host=allowed | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |
| N5b | nginx h2c up, Host=upstream-name + XFH; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6879 (master) / 6880 (branch). Identical: `:authority: gw_master`
matches nothing on either binary, `X-Forwarded-Host` is not trusted here.
#### 4.7 nginx h2c upstream, no Host override, `trust_forwarded_headers = true`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| N5c | nginx h2c up, Host=upstream-name + XFH; client Host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| N5c | nginx h2c up, Host=upstream-name + XFH; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6881 (master) / 6882 (branch). Identical and correct on both.
#### 4.8 Caddy, h2c upstream, client Host preserved, trust=false

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| C1 | caddy h2c up, Host preserved; client Host=allowed | 200 `control_any` | 200 `allowed_host` | **403** | 200 | 200 | 200 | **yes** |
| C1 | caddy h2c up, Host preserved; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6871 (master) / 6872 (branch). Caddy block:

```caddyfile
:6871 {
	reverse_proxy 127.0.0.1:6842 {
		transport http {
			versions h2c 2
		}
	}
}
```

**This is the divergence that matters.** Caddy preserves the client's host
name but puts it in `:authority` with no `Host` header, so on master the
request is hostless, the `hosts = ["panoptikon.example.com"]` policy cannot
match (`policy.rs:606-608`: a non-empty `hosts` needs `host.is_some()`), and
the request lands on the catch-all — the UI's DB route 403s for the *allowed*
host. The branch reads `:authority` and selects `allowed_host`.
#### 4.9 Caddy, h2c upstream, `Host` rewritten to the upstream (+ `X-Forwarded-Host`), trust=false

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| C2 | caddy h2c up, Host=upstream-hostport + XFH; client Host=allowed | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |
| C2 | caddy h2c up, Host=upstream-hostport + XFH; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6873 (master) / 6874 (branch), with `header_up Host {upstream_hostport}`
and `header_up X-Forwarded-Host {host}`. Identical on both: the authority is
now `127.0.0.1:6842`, which matches no policy.
#### 4.10 Caddy h2c upstream, `Host` rewritten, `trust_forwarded_headers = true`

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| C3 | caddy h2c up, Host=upstream-hostport + XFH; client Host=allowed | 200 `allowed_host` | 200 `allowed_host` | 200 | 200 | 200 | 200 | same |
| C3 | caddy h2c up, Host=upstream-hostport + XFH; client Host=denied | 200 `control_any` | 200 `control_any` | **403** | **403** | 200 | 200 | same |


Ports: 6875 (master) / 6876 (branch). Identical and correct on both.
#### 4.11 Caddy h2c upstream, client Host preserved, STRICT config (no catch-all)

| id | request shape | `/api/client-config` master | branch | `/api/db` master | branch | `/api/search/stats` master | branch | Δ |
|---|---|---|---|---|---|---|---|---|
| C4 | caddy h2c up, Host preserved (STRICT cfg); client Host=allowed | **403** | 200 `allowed_host` | **403** | 200 | **403** | 200 | **yes** |
| C4 | caddy h2c up, Host preserved (STRICT cfg); client Host=denied | **403** | **403** | **403** | **403** | **403** | **403** | same |


Ports: 6885 (master, gateway 6846) / 6886 (branch, gateway 6856) — the same
Caddy h2c block as §4.8, pointed at the strict configs. On master **every
request is 403, including `/api/client-config` and the public route**; the log
reason is `no_policy`. On the branch the allowed name works and the denied
name 403s, which is the intended behaviour.

## 5. Findings

Every request shape where master and branch disagree, with the line that
decides it and the header values that reached it.

### F1 — BRANCH FIXES: a Caddy/h2c reverse proxy makes a host-scoped policy unreachable on master

| | master | branch |
|---|---|---|
| Caddy h2c, catch-all config (§4.8 `C1`, allowed name) | 200 `control_any`, `/api/db` **403** | 200 `allowed_host`, `/api/db` 200 |
| Caddy h2c, strict config (§4.11 `C4`, allowed name) | **403** on all three routes | 200 on all three |

What reached the policy layer (`raw/h2c-upstream-headers.txt`, third block):

```
:authority: panoptikon.example.com
:method: GET
:path: /api/client-config
:scheme: http
via: 1.1 Caddy
x-forwarded-host: panoptikon.example.com
```

No `Host` header — Caddy is following RFC 9113 §8.3.1. On master
`resolve_effective_host` (`policy.rs:472-488`) ends at
`policy.rs:487`, `req.headers().get(header::HOST)` → `None` → hostless.
`select_policy` (master `policy.rs:568-582`, the `host_ok` term) requires
`host.is_some_and(...)` for a non-empty `hosts`, so `allowed_host` is skipped
and the request either lands on the catch-all or, with no catch-all, is
refused at master `policy.rs:249-252` with `reason = "no_policy"`. Log
(`raw/policy-log-excerpts.txt`, first block):

```
WARN panoptikon::policy: request denied: policy method=GET path=/api/db reason="no_policy"
WARN panoptikon::policy: request denied: policy method=GET path=/api/search/stats reason="no_policy"
```

On the branch, `policy.rs:514` calls `request_authority` (`policy.rs:472-484`),
which returns the URI authority hyper built from `:authority`, and the same
request logs

```
DEBUG panoptikon::policy: policy enforced method=GET path=/api/client-config policy=allowed_host selected_by=listener/host endpoint="default" db_params=skipped status=200 OK
```

This is P1 in its reverse-proxy form, and it is the run's main result: on
master, a user who puts Caddy (or any h2c-correct proxy) in front of the
gateway and writes a `hosts = [...]` policy gets a gateway that denies
everything, with no configuration that works around it short of turning on
`trust_forwarded_headers`.

### F2 — BRANCH ALLOWS what master denied: the authority now outranks `Host`

Shapes `D5`, `D7`, `D9`, `C1`, `F5`. All four are the same rule: whenever the
request target carries an authority, the branch uses it and master used
`Host`.

| shape | `:authority` / absolute-form target | `Host` | master | branch |
|---|---|---|---|---|
| `D5` | `panoptikon.example.com` (h1.1 absolute-form) | `denied.example.net` | `control_any` / 403 | `allowed_host` / 200 |
| `D7` | `panoptikon.example.com:6842` (h2c) | *absent* | `control_any` / 403 | `allowed_host` / 200 |
| `D9` | `panoptikon.example.com` (h2c) | `denied.example.net` | `control_any` / 403 | `allowed_host` / 200 |
| `F5` | `panoptikon.example.com` (h2c) | *absent*, `X-Forwarded-Host: denied.example.net`, trust=false | `control_any` | `allowed_host` |

Deciding line: `policy.rs:514` vs master's `policy.rs:487`. `D7` is the
intended fix. `D5`/`D9` are the malformed-and-disagreeing case that RFC 9113
§8.3.1 forbids a client from sending at all and RFC 9112 §3.2.2 resolves in
the authority's favour on HTTP/1.1; the branch's own comment
(`policy.rs:1280-1283`) states this is deliberate. `F5` is `D7` with a
forwarded header the gateway was told not to trust.

**This is a widening of what a `hosts` policy matches, not a privilege
escalation**: both names are chosen by the same client, and the branch's doc
(`docs/inferio-transport.md` "Request authority") and
`config/server/default.toml:246-256` both say `hosts` is routing convenience
while `endpoints` is the non-spoofable dimension. §3.5 and §4.4 confirm
`endpoints` did not move.

### F3 — BRANCH DENIES what master allowed: `Host` no longer overrides the authority

Shapes `D6` and `D10`.

| shape | `:authority` / absolute-form target | `Host` | master | branch |
|---|---|---|---|---|
| `D6` | `denied.example.net` (h1.1 absolute-form) | `panoptikon.example.com` | `allowed_host` / 200 | `control_any` / **403** |
| `D10` | `denied.example.net` (h2c) | `panoptikon.example.com` | `allowed_host` / 200 | `control_any` / **403** |

Same two lines decide it, in the other direction. On the strict config both
become an outright 403 `no_policy` on the branch where master answered 200.

Real-world exposure is small but not zero: it needs a client or proxy that
sets an authority naming *one* host and a `Host` header naming *another*. None
of the four proxy shapes measured here does that — nginx h2c with
`proxy_set_header Host $host` sends `host` and **no** `:authority`
(`raw/h2c-upstream-headers.txt`, first block), which is why §4.5 is identical
on both binaries; nginx h2c without the override sends `:authority` and no
`host`. A hand-rolled h2 client, or a forward proxy used the way `curl
--proxy` is used in `D5`/`D6`, can produce it. It is a behaviour reversal that
neither `panoptikon/README.md` nor `docs/inferio-transport.md` frames as one —
both describe the new precedence as if it had always applied.

### F4 — BRANCH ALLOWS what master denied: `normalize_host` now strips `userinfo@`

Shapes `U1` and `U3` (§3.4).

```
curl -sS --http1.1 -H 'Host: evil.example.net@panoptikon.example.com' \
     http://127.0.0.1:6846/api/client-config      # master strict → 403
curl -sS --http1.1 -H 'Host: evil.example.net@panoptikon.example.com' \
     http://127.0.0.1:6856/api/client-config      # branch strict → 200 allowed_host
```

Decided by `policy.rs:540-546` on the branch:

```rust
let value = match value.rfind('@') {
    Some(at) => &value[at + 1..],
    None => value,
};
```

which master's `normalize_host` (`policy.rs:510-522`) does not have — master
lowercases the whole string and compares it whole, so it matches nothing.
`U2` (`panoptikon.example.com@denied.example.net`) is `control_any` / 403 on
both, i.e. the strip takes the host *after* the last `@`, which is RFC 3986
§3.2.1 and matches what `http::uri::Authority::host` would have returned for
the same string. So the branch is the more correct of the two and the two
paths (authority and `Host`) now agree with each other.

It is still a widening of `hosts` matching that arrived alongside an
unrelated fix and is documented only in the function's own comment — the
precedence lists in `panoptikon/README.md:65-76` and
`config/server/default.toml:245-256` describe *which source* is read, never
that the string is now trimmed at `@`. Low severity for the same reason as F2.

### N1 — NOT a finding: the trust_forwarded_headers deployment is unchanged

With `trust_forwarded_headers = true`, master and branch agree on **every**
shape measured: `F1`-`F6` direct (§3.3), `N2` (§4.3), `N5c` (§4.7) and `C3`
(§4.10) through both proxies. `resolve_effective_host` returns from the
forwarded branch (`policy.rs:500-512`) before the changed line runs.

This is the deployment the shipped configs point a proxied user at:
`config/server/default.toml:19` and `config/server/docker.toml:41` both set
`trust_forwarded_headers = true` as a **live line**. The two profiles that do
*not* — `nixos.toml:17` ships it commented, and `desktop.toml` /
`desktop-dev.toml` omit it entirely, so both take the `#[serde(default)]`
`false` at `config.rs:809-810` — are exactly where F1 bites. A NixOS or
Desktop-managed gateway behind Caddy, on master, is the broken case.

### N2 — NOT a finding: `[[server.endpoints]]` is untouched

§3.5 (direct) and §4.4 (through nginx): all four host shapes × three routes ×
both binaries answer 200 under policy `legacy_endpoint`, including the shapes
that are hostless. The endpoint dimension never consults
`resolve_effective_host`; it reads the `ListenerEndpoint` extension at
`policy.rs:236-239` and compares at `policy.rs:611-614`.

## 6. Artifacts and how to re-run

```
tools/calibration-protocol/results/run4-proxy/
├── report.md                      this file
├── configs/
│   ├── m_trustfalse.toml  m_trusttrue.toml  m_strict.toml
│   ├── b_trustfalse.toml  b_trusttrue.toml  b_strict.toml
│   ├── nginx.conf                 every nginx server block used
│   └── Caddyfile                  every Caddy site used
├── raw/
│   ├── direct.tsv                 §3.1 §3.2 §3.3 §3.5, 192 measurements
│   ├── proxy.tsv                  §4.1-§4.10, 120 measurements
│   ├── strict-proxy.tsv           §4.11, 12 measurements
│   ├── extra.tsv                  §3.4, 20 measurements
│   ├── nginx-upstream-headers.txt what nginx sends upstream over h1.1
│   ├── h2c-upstream-headers.txt   what nginx and Caddy send upstream over h2c
│   └── policy-log-excerpts.txt    the policy layer's own decision lines
└── tools/
    ├── start.sh                   brings up the six gateways
    ├── lib.sh  run_direct.sh  run_proxy.sh  extra.sh
    ├── h2req.py                   h2c client with independent :authority / host
    ├── h2dump.py                  h2c server that prints the headers it received
    └── rawdump.py                 h1 server that prints the bytes it received
```

Re-run:

```sh
export RUN4_PROXY_ROOT=$HOME/tmp-run4-proxy CUDA_VISIBLE_DEVICES="" TMPDIR=$HOME/tmp-run4-proxy
# configs/*.toml use ${RUN4_PROXY_ROOT} for data_folder and the log file;
# substitute it, then:
tools/start.sh                                   # six gateways, ports 6842-6857
python3 -m venv h2venv && ./h2venv/bin/pip install h2
docker run -d --name run4rp-nginx --network host \
  -v $PWD/configs/nginx.conf:/etc/nginx/nginx.conf:ro nginx:1.29
docker run -d --name run4rp-caddy --network host \
  -v $PWD/configs/Caddyfile:/etc/caddy/Caddyfile:ro caddy:2
tools/run_direct.sh > raw/direct.tsv
tools/run_proxy.sh  > raw/proxy.tsv
tools/extra.sh      > raw/extra.tsv
docker rm -f run4rp-nginx run4rp-caddy
```

Each gateway needs its own `--root`; the six in `tools/start.sh` use
`$HOME/tmp-run4-proxy/<name>` because the server takes an exclusive
`runtime/server.lock` per root.

## 7. Summary

- 344 recorded responses across 6 gateway configurations, 2 binaries,
  4 proxy transports and 3 routes.
- **The reverse-proxy deployment the shipped configs describe
  (`trust_forwarded_headers = true`) is byte-identical between master and the
  branch** — every forwarded shape, both proxies, both HTTP versions.
- The branch **fixes** a real break for a proxied user who does not turn that
  on: an h2c-correct front proxy (Caddy's default) leaves master's policy
  layer hostless and every host-scoped policy unreachable — 403 `no_policy` on
  every route (F1). `nixos.toml` and both `desktop*.toml` profiles run with
  `trust_forwarded_headers` off, so this is reachable on shipped
  configuration.
- The branch **widens** `hosts` matching in three shapes (F2, F4) and
  **narrows** it in two (F3), all of them cases where a client sends an
  authority and a `Host` that disagree, or a `userinfo@` prefix. All are
  RFC-correct and none crosses the `endpoints` boundary, which is unchanged
  (N2) — but F3 is a behaviour reversal and F4 is a string-normalization
  change, and neither is called out as such in `panoptikon/README.md` or
  `docs/inferio-transport.md`.
- `[[server.endpoints]]` legacy-port behaviour is unchanged, direct and behind
  nginx.
