package com.entrodb.server;

import com.entrodb.executor.*;
import com.entrodb.sql.*;
import com.entrodb.sql.ast.Statement;

import java.io.*;
import java.net.Socket;

public class ClientHandler implements Runnable {

    private final Socket         socket;
    private final QueryExecutor  executor;
    private final SessionContext session;
    private final AuthManager    authManager;
    private final StatsCollector stats;

    public ClientHandler(Socket socket, QueryExecutor executor,
                          AuthManager authManager, StatsCollector stats) {
        this.socket      = socket;
        this.executor    = executor;
        this.session     = new SessionContext(socket);
        this.authManager = authManager;
        this.stats       = stats;
    }

    @Override
    public void run() {
        System.out.printf("[+] Connection from %s%n",
            session.getClientAddress());
        stats.clientConnected();

        try (
            InputStream  in  = socket.getInputStream();
            OutputStream out = socket.getOutputStream()
        ) {
            EntroProtocol.writeResponse(out,
                EntroProtocol.STATUS_AUTH_REQ,
                "EntroDB v1.0.0 — send: username:password");

            String   credentials = EntroProtocol.readRequest(in);
            String[] parts       = credentials.split(":", 2);

            if (parts.length != 2
                    || !authManager.authenticate(parts[0], parts[1])) {
                EntroProtocol.writeResponse(out,
                    EntroProtocol.STATUS_AUTH_FAIL,
                    "Authentication failed");
                System.out.printf("[-] Auth failed from %s%n",
                    session.getClientAddress());
                return;
            }

            session.setUsername(parts[0]);
            EntroProtocol.writeResponse(out,
                EntroProtocol.STATUS_AUTH_OK,
                "Welcome " + parts[0]
                + " | session=" + session.getSessionId());

            System.out.printf("[+] Authenticated: %s %s%n",
                parts[0], session.getSummary());

            while (session.isActive()) {
                String sql;
                try { sql = EntroProtocol.readRequest(in); }
                catch (EOFException e) { break; }

                sql = sql.trim();
                if (sql.isEmpty()) continue;

                if (sql.equalsIgnoreCase("\\quit")
                        || sql.equalsIgnoreCase("\\q")) {
                    EntroProtocol.writeResponse(out,
                        EntroProtocol.STATUS_GOODBYE, "Bye!");
                    break;
                }
                if (sql.equalsIgnoreCase("\\ping")) {
                    EntroProtocol.writeOK(out, "PONG"); continue;
                }
                if (sql.equalsIgnoreCase("\\status")) {
                    EntroProtocol.writeOK(out, session.getSummary()); continue;
                }

                long   start  = System.currentTimeMillis();
                String result = executeSQL(sql);
                long   ms     = System.currentTimeMillis() - start;
                boolean error = result.startsWith("ERROR");

                stats.recordQuery(sql, ms, error, session.getUsername());
                session.incrementQueryCount();
                EntroProtocol.writeOK(out, result);
            }

        } catch (IOException e) {
            System.err.printf("[-] Client error %s: %s%n",
                session.getClientAddress(), e.getMessage());
        } finally {
            stats.clientDisconnected();
            session.close();
            try { socket.close(); } catch (IOException ignored) {}
            System.out.printf("[-] Disconnected: %s %s%n",
                session.getUsername(), session.getSummary());
        }
    }

    private String executeSQL(String sql) {
        long start = System.currentTimeMillis();
        try {
            Lexer     lexer  = new Lexer(sql);
            Parser    parser = new Parser(lexer.tokenize());
            Statement stmt   = parser.parse();
            ResultSet result = executor.execute(stmt);
            long ms = System.currentTimeMillis() - start;
            return result.prettyPrint() + String.format("Time: %dms", ms);
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }
}
