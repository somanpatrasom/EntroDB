package com.entrodb.server;

import java.io.*;
import java.net.Socket;

/**
 * EntroDB Java Client
 *
 * Usage:
 *   NexusClient client = new NexusClient("localhost", 6969);
 *   client.connect();
 *   String result = client.query("SELECT * FROM users;");
 *   System.out.println(result);
 *   client.close();
 */
public class NexusClient implements Closeable {

    private final String host;
    private final int    port;

    private Socket       socket;
    private InputStream  in;
    private OutputStream out;
    private boolean      connected = false;

    public NexusClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        in  = socket.getInputStream();
        out = socket.getOutputStream();

        // Read welcome banner
        Response welcome = readResponse();
        if (welcome.status() != EntroProtocol.STATUS_OK) {
            throw new IOException("Server rejected connection: " + welcome.payload());
        }
        connected = true;
        System.out.println("Connected to EntroDB: " + welcome.payload().trim());
    }

    /**
     * Send a SQL query and return the server's response as a string.
     * Throws IOException on network error, RuntimeException on SQL error.
     */
    public String query(String sql) throws IOException {
        if (!connected) throw new IllegalStateException("Not connected");

        // Send request
        byte[] sqlBytes = sql.getBytes("UTF-8");
        ByteArrayOutputStream buf = new ByteArrayOutputStream(4 + sqlBytes.length);
        DataOutputStream dos = new DataOutputStream(buf);
        dos.writeInt(sqlBytes.length);
        dos.write(sqlBytes);
        out.write(buf.toByteArray());
        out.flush();

        // Read response
        Response resp = readResponse();
        if (resp.status() == EntroProtocol.STATUS_ERROR) {
            throw new RuntimeException(resp.payload());
        }
        return resp.payload();
    }

    public boolean ping() {
        try {
            byte[] pingBytes = "\\ping".getBytes("UTF-8");
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(buf);
            dos.writeInt(pingBytes.length);
            dos.write(pingBytes);
            out.write(buf.toByteArray());
            out.flush();
            Response r = readResponse();
            return "PONG".equals(r.payload());
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        if (connected) {
            try {
                byte[] quitBytes = "\\quit".getBytes("UTF-8");
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(buf);
                dos.writeInt(quitBytes.length);
                dos.write(quitBytes);
                out.write(buf.toByteArray());
                out.flush();
            } catch (IOException ignored) {}
        }
        connected = false;
        if (socket != null) socket.close();
    }

    private Response readResponse() throws IOException {
        // Read status byte
        int status = in.read();
        if (status == -1) throw new EOFException("Server disconnected");

        // Read 4-byte length
        byte[] lenBytes = new byte[4];
        int r = 0;
        while (r < 4) {
            int n = in.read(lenBytes, r, 4 - r);
            if (n == -1) throw new EOFException("Server disconnected mid-read");
            r += n;
        }
        int length = new DataInputStream(
            new ByteArrayInputStream(lenBytes)).readInt();

        // Read payload
        byte[] payload = new byte[length];
        int read = 0;
        while (read < length) {
            int n = in.read(payload, read, length - read);
            if (n == -1) throw new EOFException("Server disconnected mid-payload");
            read += n;
        }

        return new Response((byte) status, new String(payload, "UTF-8"));
    }

    public record Response(byte status, String payload) {}
}
