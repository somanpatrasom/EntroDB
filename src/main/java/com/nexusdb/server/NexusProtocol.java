package com.nexusdb.server;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * NexusDB Wire Protocol v1
 *
 * REQUEST  (client → server):
 *   [4 bytes: payload length][N bytes: UTF-8 SQL string]
 *
 * RESPONSE (server → client):
 *   [1 byte: status][4 bytes: payload length][N bytes: UTF-8 payload]
 *
 * Status bytes:
 *   0x00 = OK
 *   0x01 = ERROR
 *   0x02 = GOODBYE (server closing connection)
 */
public class NexusProtocol {

    public static final byte STATUS_OK      = 0x00;
    public static final byte STATUS_ERROR   = 0x01;
    public static final byte STATUS_GOODBYE = 0x02;

    // ── Writing ───────────────────────────────────────────────────

    public static void writeResponse(OutputStream out, byte status, String payload)
            throws IOException {
        byte[] payloadBytes = payload.getBytes("UTF-8");
        ByteBuffer buf = ByteBuffer.allocate(5 + payloadBytes.length);
        buf.put(status);
        buf.putInt(payloadBytes.length);
        buf.put(payloadBytes);
        out.write(buf.array());
        out.flush();
    }

    public static void writeOK(OutputStream out, String payload) throws IOException {
        writeResponse(out, STATUS_OK, payload);
    }

    public static void writeError(OutputStream out, String message) throws IOException {
        writeResponse(out, STATUS_ERROR, "ERROR: " + message);
    }

    // ── Reading ───────────────────────────────────────────────────

    public static String readRequest(InputStream in) throws IOException {
        // Read 4-byte length prefix
        byte[] lenBytes = readExactly(in, 4);
        int length = ByteBuffer.wrap(lenBytes).getInt();

        if (length <= 0 || length > 1_048_576) { // max 1MB query
            throw new IOException("Invalid request length: " + length);
        }

        byte[] payload = readExactly(in, length);
        return new String(payload, "UTF-8");
    }

    // ── Helpers ───────────────────────────────────────────────────

    private static byte[] readExactly(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int read = 0;
        while (read < n) {
            int r = in.read(buf, read, n - read);
            if (r == -1) throw new EOFException("Connection closed mid-read");
            read += r;
        }
        return buf;
    }
}
