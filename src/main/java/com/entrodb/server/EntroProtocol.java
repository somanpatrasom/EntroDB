package com.entrodb.server;

import java.io.*;
import java.nio.ByteBuffer;

public class EntroProtocol {

    public static final byte STATUS_OK        = 0x00;
    public static final byte STATUS_ERROR     = 0x01;
    public static final byte STATUS_GOODBYE   = 0x02;
    public static final byte STATUS_AUTH_REQ  = 0x03; // server requests auth
    public static final byte STATUS_AUTH_OK   = 0x04; // auth succeeded
    public static final byte STATUS_AUTH_FAIL = 0x05; // auth failed

    public static void writeResponse(OutputStream out, byte status,
                                      String payload) throws IOException {
        byte[] payloadBytes = payload.getBytes("UTF-8");
        ByteBuffer buf = ByteBuffer.allocate(5 + payloadBytes.length);
        buf.put(status);
        buf.putInt(payloadBytes.length);
        buf.put(payloadBytes);
        out.write(buf.array());
        out.flush();
    }

    public static void writeOK(OutputStream out, String payload)
            throws IOException {
        writeResponse(out, STATUS_OK, payload);
    }

    public static void writeError(OutputStream out, String message)
            throws IOException {
        writeResponse(out, STATUS_ERROR, "ERROR: " + message);
    }

    public static String readRequest(InputStream in) throws IOException {
        byte[] lenBytes = readExactly(in, 4);
        int length = ByteBuffer.wrap(lenBytes).getInt();
        if (length <= 0 || length > 1_048_576)
            throw new IOException("Invalid request length: " + length);
        return new String(readExactly(in, length), "UTF-8");
    }

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
