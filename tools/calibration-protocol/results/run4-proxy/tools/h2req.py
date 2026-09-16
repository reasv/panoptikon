#!/usr/bin/env python3
"""Minimal h2c (HTTP/2 prior-knowledge cleartext) client that lets the caller
set :authority and a literal `host` header independently.

usage: h2req.py HOST PORT PATH [--authority A] [--host H] [--xfh V] [--fwd V]
prints: "<status> <body>"
"""
import socket, sys, json
from h2.connection import H2Connection
from h2.config import H2Configuration
from h2.events import ResponseReceived, DataReceived, StreamEnded

a = sys.argv[1:]
host, port, path = a[0], int(a[1]), a[2]
opt = {}
i = 3
while i < len(a):
    opt[a[i].lstrip('-')] = a[i + 1]
    i += 2

hdrs = [
    (':method', 'GET'),
    (':path', path),
    (':scheme', 'http'),
]
if 'authority' in opt:
    hdrs.append((':authority', opt['authority']))
if 'host' in opt:
    hdrs.append(('host', opt['host']))
if 'xfh' in opt:
    hdrs.append(('x-forwarded-host', opt['xfh']))
if 'fwd' in opt:
    hdrs.append(('forwarded', opt['fwd']))
hdrs.append(('user-agent', 'run4-proxy-h2req/1'))

cfg = H2Configuration(client_side=True, header_encoding='utf-8',
                      validate_outbound_headers=False,
                      normalize_outbound_headers=False,
                      validate_inbound_headers=False,
                      normalize_inbound_headers=False)
c = H2Connection(config=cfg)
s = socket.create_connection((host, port), timeout=10)
c.initiate_connection()
s.sendall(c.data_to_send())
c.send_headers(1, hdrs, end_stream=True)
s.sendall(c.data_to_send())

status, body, done = None, b'', False
while not done:
    d = s.recv(65535)
    if not d:
        break
    for ev in c.receive_data(d):
        if isinstance(ev, ResponseReceived):
            status = dict(ev.headers).get(':status')
        elif isinstance(ev, DataReceived):
            body += ev.data
            c.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
        elif isinstance(ev, StreamEnded):
            done = True
    out = c.data_to_send()
    if out:
        s.sendall(out)
s.close()
txt = body.decode('utf-8', 'replace')
try:
    txt = json.loads(txt).get('policy', txt[:80])
except Exception:
    txt = txt[:80].replace('\n', ' ')
print(f"{status} {txt}")
