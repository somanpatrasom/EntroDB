#!/usr/bin/env python3
"""
EntroDB Python Client v2 — with authentication
Usage:
    python3 nexus_client.py [host] [port] [username] [password]
    python3 nexus_client.py localhost 6969 admin entrodb
"""

import socket
import struct
import sys

STATUS_OK        = 0x00
STATUS_ERROR     = 0x01
STATUS_GOODBYE   = 0x02
STATUS_AUTH_REQ  = 0x03
STATUS_AUTH_OK   = 0x04
STATUS_AUTH_FAIL = 0x05


class EntroClient:

    def __init__(self, host="localhost", port=6969,
                 username="admin", password="entrodb"):
        self.host     = host
        self.port     = port
        self.username = username
        self.password = password
        self.sock     = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))

        # Read auth request
        status, payload = self._read_response()
        if status != STATUS_AUTH_REQ:
            raise ConnectionError(f"Expected auth request, got: {payload}")

        # Send credentials
        credentials = f"{self.username}:{self.password}"
        self._send(credentials)

        # Read auth result
        status, payload = self._read_response()
        if status == STATUS_AUTH_FAIL:
            raise ConnectionError(f"Authentication failed: {payload}")
        if status != STATUS_AUTH_OK:
            raise ConnectionError(f"Unexpected auth response: {payload}")

        print(f"Connected: {payload.strip()}")

    def query(self, sql: str) -> str:
        self._send(sql)
        status, payload = self._read_response()
        if status == STATUS_ERROR:
            raise RuntimeError(payload)
        return payload

    def ping(self) -> bool:
        try:
            return self.query("\\ping").strip() == "PONG"
        except Exception:
            return False

    def close(self):
        if self.sock:
            try: self.query("\\quit")
            except Exception: pass
            self.sock.close()
            self.sock = None

    def _send(self, text: str):
        b = text.encode("utf-8")
        self.sock.sendall(struct.pack(">I", len(b)) + b)

    def _read_response(self):
        header  = self._recv_exact(5)
        status  = header[0]
        length  = struct.unpack(">I", header[1:5])[0]
        payload = self._recv_exact(length).decode("utf-8")
        return status, payload

    def _recv_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk: raise EOFError("Server disconnected")
            buf += chunk
        return buf


def main():
    host     = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port     = int(sys.argv[2]) if len(sys.argv) > 2 else 6969
    username = sys.argv[3] if len(sys.argv) > 3 else "admin"
    password = sys.argv[4] if len(sys.argv) > 4 else "entrodb"

    client = EntroClient(host, port, username, password)
    client.connect()
    print("EntroDB Python Client | type \\q to quit\n")

    buf = []
    while True:
        try:
            line = input("entro> " if not buf else "   ... ").strip()
        except (EOFError, KeyboardInterrupt):
            print(); break

        if line in ("\\q", "\\quit"): break
        buf.append(line)
        full = " ".join(buf)

        if full.rstrip().endswith(";"):
            sql = full.strip().rstrip(";").strip()
            buf = []
            if not sql:
                continue
            try:    print(client.query(full))
            except RuntimeError as e: print(f"Error: {e}")

    client.close()
    print("Disconnected.")


if __name__ == "__main__":
    main()
