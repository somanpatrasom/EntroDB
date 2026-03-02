package com.nexusdb.server;

import com.nexusdb.executor.QueryExecutor;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The NexusDB TCP Server.
 *
 * - Listens on port 6969 (configurable)
 * - One thread per client (thread pool, max 50 concurrent)
 * - Graceful shutdown on SIGINT
 *
 * To connect:
 *   Use NexusClient.java (Java)
 *   Use nexus_client.py (Python)
 *   Or raw socket with the wire protocol
 */
public class NexusServer {

    public static final int DEFAULT_PORT    = 6969;
    public static final int MAX_CONNECTIONS = 50;

    private final int port;
    private final QueryExecutor executor;
    private final ExecutorService threadPool;
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public NexusServer(QueryExecutor executor) {
        this(DEFAULT_PORT, executor);
    }

    public NexusServer(int port, QueryExecutor executor) {
        this.port       = port;
        this.executor   = executor;
        this.threadPool = Executors.newFixedThreadPool(MAX_CONNECTIONS,
            r -> {
                Thread t = new Thread(r);
                t.setName("nexus-client-" + activeConnections.incrementAndGet());
                t.setDaemon(true);
                return t;
            });
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        running = true;

        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("  NexusDB Server started");
        System.out.printf ("  Listening on port %d%n", port);
        System.out.printf ("  Max connections: %d%n", MAX_CONNECTIONS);
        System.out.println("  Press Ctrl+C to stop");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        while (running) {
            try {
                Socket client = serverSocket.accept();
                client.setTcpNoDelay(true);          // disable Nagle's algorithm
                client.setSoTimeout(300_000);         // 5 min idle timeout
                threadPool.submit(new ClientHandler(client, executor));
            } catch (IOException e) {
                if (running) System.err.println("Accept error: " + e.getMessage());
            }
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}
        System.out.println("NexusDB Server stopped.");
    }
}
