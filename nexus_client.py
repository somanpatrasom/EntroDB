#!/usr/bin/env python3
"""
NexusDB Python Client
Usage:
    client = NexusClient("localhost", 6969)
    client.connect()
    print(client.query("SELECT * FROM users;"))
    client.close()

Or as a CLI:
    python3 nexus_client.py
"""

import socket
import struct
import sys


class NexusClient:
    STATUS_OK      = 0x00
    STATUS_ERROR   = 0x01
    STATUS_GOODBYE = 0x02

    def __init__(self, host="localhost", port=6969):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))

        status, payload = self._read_response()
        if status != self.STATUS_OK:
            raise ConnectionError(f"Server rejected: {payload}")
        print(f"Connected: {payload.strip()}")

    def query(self, sql: str) -> str:
        """Send SQL, return result string. Raises on error."""
        sql_bytes = sql.encode("utf-8")
        # [4 bytes length][sql bytes]
        self.sock.sendall(struct.pack(">I", len(sql_bytes)) + sql_bytes)

        status, payload = self._read_response()
        if status == self.STATUS_ERROR:
            raise RuntimeError(payload)
        if status == self.STATUS_GOODBYE:
            return payload
        return payload

    def ping(self) -> bool:
        try:
            result = self.query("\\ping")
            return result.strip() == "PONG"
        except Exception:
            return False

    def close(self):
        if self.sock:
            try:
                self.query("\\quit")
            except Exception:
                pass
            self.sock.close()
            self.sock = None

    def _read_response(self):
        # Read status (1 byte) + length (4 bytes)
        header = self._recv_exact(5)
        status = header[0]
        length = struct.unpack(">I", header[1:5])[0]
        payload = self._recv_exact(length).decode("utf-8")
        return status, payload

    def _recv_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("Server disconnected")
            buf += chunk
        return buf


# ── Interactive CLI ───────────────────────────────────────────────────────────

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6969

    client = NexusClient(host, port)
    client.connect()

    print("NexusDB Python Client | type \\q to quit\n")
    buf = []

    while True:
        try:
            prompt = "nexus> " if not buf else "   ... "
            line = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if line in ("\\q", "\\quit"):
            break

        buf.append(line)
        full = " ".join(buf)

        if full.rstrip().endswith(";"):
            buf = []
            try:
                print(client.query(full))
            except RuntimeError as e:
                print(f"Error: {e}")

    client.close()
    print("Disconnected.")


if __name__ == "__main__":
    main()
