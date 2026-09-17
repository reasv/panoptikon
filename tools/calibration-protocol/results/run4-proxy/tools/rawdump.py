import socket, sys
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 6890)); s.listen(5)
n = int(sys.argv[1])
for _ in range(n):
    c, _a = s.accept()
    c.settimeout(3)
    data = b''
    try:
        while b'\r\n\r\n' not in data:
            b = c.recv(4096)
            if not b: break
            data += b
    except Exception: pass
    print("---- upstream request as nginx sent it ----")
    print(data.decode('latin1').split('\r\n\r\n')[0])
    c.sendall(b'HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
    c.close()
s.close()
