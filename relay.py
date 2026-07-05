#!/usr/bin/env python3
"""Relay minimale per Hexotem: stanze da 2 giocatori con codice, inoltro messaggi.

Solo libreria standard: deployabile ovunque giri Python 3 (Render, Fly, un VPS...).
Avvio:  python3 relay.py   (porta da $PORT, default 9090)

Protocollo (JSON su WebSocket):
  client -> server: {"op":"create"} | {"op":"join","code":"ABCD"} | {"op":"queue"}
                    | {"op":"msg","data":"..."}
  server -> client: {"op":"created","code":"ABCD"} | {"op":"start","role":"A"|"B"}
                    | {"op":"msg","data":"..."} | {"op":"peer_left"} | {"op":"error","reason":"..."}
"queue" = matchmaking automatico: i primi due client in coda vengono accoppiati.
Il campo "data" e' opaco: il relay non lo interpreta mai.
"""
import base64
import hashlib
import json
import os
import random
import socket
import threading

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
rooms = {}       # code -> [Client, ...]
queue = []       # client in attesa di matchmaking automatico
rooms_lock = threading.Lock()


class Client:
    def __init__(self, sock):
        self.sock = sock
        self.code = None
        self.send_lock = threading.Lock()

    # --- framing websocket (RFC 6455, il minimo che serve) ---
    def recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError
            buf += chunk
        return buf

    def recv_message(self):
        data = b""
        while True:
            b1, b2 = self.recv_exact(2)
            fin, opcode = b1 & 0x80, b1 & 0x0F
            length = b2 & 0x7F
            if length == 126:
                length = int.from_bytes(self.recv_exact(2), "big")
            elif length == 127:
                length = int.from_bytes(self.recv_exact(8), "big")
            mask = self.recv_exact(4) if b2 & 0x80 else None
            payload = self.recv_exact(length)
            if mask:
                payload = bytes(c ^ mask[i % 4] for i, c in enumerate(payload))
            if opcode == 8:      # close
                raise ConnectionError
            if opcode == 9:      # ping -> pong
                self.send_frame(payload, opcode=10)
                continue
            if opcode == 10:     # pong
                continue
            data += payload
            if fin:
                return data

    def send_frame(self, payload, opcode=1):
        header = bytes([0x80 | opcode])
        n = len(payload)
        if n < 126:
            header += bytes([n])
        elif n < 65536:
            header += bytes([126]) + n.to_bytes(2, "big")
        else:
            header += bytes([127]) + n.to_bytes(8, "big")
        with self.send_lock:
            self.sock.sendall(header + payload)

    def send(self, obj):
        try:
            self.send_frame(json.dumps(obj).encode())
        except OSError:
            pass


def handshake(client):
    req = b""
    while b"\r\n\r\n" not in req:
        chunk = client.sock.recv(4096)
        if not chunk:
            return False
        req += chunk
    key = None
    for line in req.decode(errors="replace").split("\r\n"):
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
    if key is None:  # richiesta HTTP normale: risposta per gli health check
        client.sock.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHexotem relay")
        return False
    accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
    client.sock.sendall((
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode())
    return True


def peers_of(client):
    with rooms_lock:
        return [c for c in rooms.get(client.code, []) if c is not client]


def handle(client):
    try:
        if not handshake(client):
            return
        while True:
            try:
                msg = json.loads(client.recv_message().decode())
            except (ValueError, UnicodeDecodeError):
                continue
            op = msg.get("op")
            if op == "create":
                with rooms_lock:
                    while True:
                        code = "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(4))
                        if code not in rooms:
                            break
                    rooms[code] = [client]
                    client.code = code
                client.send({"op": "created", "code": code})
            elif op == "join":
                code = str(msg.get("code", "")).upper()
                with rooms_lock:
                    room = rooms.get(code)
                    ok = room is not None and len(room) == 1
                    if ok:
                        room.append(client)
                        client.code = code
                if ok:
                    room[0].send({"op": "start", "role": "A"})
                    client.send({"op": "start", "role": "B"})
                else:
                    client.send({"op": "error", "reason": "Codice non valido o partita gia' piena."})
            elif op == "queue":
                paired = None
                with rooms_lock:
                    if client in queue:
                        continue
                    if queue:
                        paired = queue.pop(0)
                        code = "@" + "".join(random.choice("0123456789") for _ in range(6))
                        rooms[code] = [paired, client]
                        paired.code = code
                        client.code = code
                    else:
                        queue.append(client)
                if paired:
                    paired.send({"op": "start", "role": "A"})
                    client.send({"op": "start", "role": "B"})
            elif op == "msg":
                for peer in peers_of(client):
                    peer.send({"op": "msg", "data": msg.get("data", "")})
    except (ConnectionError, OSError):
        pass
    finally:
        for peer in peers_of(client):
            peer.send({"op": "peer_left"})
        with rooms_lock:
            rooms.pop(client.code, None)
            if client in queue:
                queue.remove(client)
        try:
            client.sock.close()
        except OSError:
            pass


def main():
    port = int(os.environ.get("PORT", "9090"))
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(8)
    print("Hexotem relay in ascolto sulla porta %d" % port)
    while True:
        sock, _addr = srv.accept()
        threading.Thread(target=handle, args=(Client(sock),), daemon=True).start()


if __name__ == "__main__":
    main()
