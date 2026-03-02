package com.nexusdb.server;

import java.net.Socket;
import java.util.UUID;

/**
 * Holds state for a single connected client session.
 * Will hold transaction state, auth info, etc. in future phases.
 */
public class SessionContext {

    private final String sessionId;
    private final String clientAddress;
    private final long connectedAt;
    private volatile boolean active;
    private long queryCount;

    public SessionContext(Socket socket) {
        this.sessionId   = UUID.randomUUID().toString().substring(0, 8);
        this.clientAddress = socket.getInetAddress().getHostAddress()
                           + ":" + socket.getPort();
        this.connectedAt = System.currentTimeMillis();
        this.active      = true;
        this.queryCount  = 0;
    }

    public void incrementQueryCount() { queryCount++; }
    public void close() { active = false; }

    public String getSessionId()    { return sessionId; }
    public String getClientAddress(){ return clientAddress; }
    public boolean isActive()       { return active; }
    public long getQueryCount()     { return queryCount; }

    public String getSummary() {
        long uptime = (System.currentTimeMillis() - connectedAt) / 1000;
        return String.format("[session=%s addr=%s queries=%d uptime=%ds]",
            sessionId, clientAddress, queryCount, uptime);
    }
}
