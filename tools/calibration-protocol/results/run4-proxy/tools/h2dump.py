import socket, sys
from h2.connection import H2Connection
from h2.config import H2Configuration
from h2.events import RequestReceived, StreamEnded
n = int(sys.argv[1]); port = int(sys.argv[2])
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', port)); s.listen(5)
seen = 0
while seen < n:
    c, _ = s.accept(); c.settimeout(5)
    cfg = H2Configuration(client_side=False, header_encoding='utf-8',
                          validate_inbound_headers=False, normalize_inbound_headers=False)
    conn = H2Connection(config=cfg); conn.initiate_connection(); c.sendall(conn.data_to_send())
    try:
        while seen < n:
            d = c.recv(65535)
            if not d: break
            for ev in conn.receive_data(d):
                if isinstance(ev, RequestReceived):
                    seen += 1
                    print("---- h2c request as the proxy sent it ----")
                    for k, v in ev.headers:
                        print(f"{k}: {v}")
                    conn.send_headers(ev.stream_id, [(':status', '204')], end_stream=True)
            out = conn.data_to_send()
            if out: c.sendall(out)
    except Exception:
        pass
    c.close()
s.close()
