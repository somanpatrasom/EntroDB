package com.nexusdb.server;

import com.nexusdb.executor.*;
import com.nexusdb.sql.*;
import com.nexusdb.sql.ast.Statement;

import java.io.*;
import java.net.Socket;

/**
 * Handles one client connection on its own thread.
 *
 * Loop:
 *   1. Read SQL request via NexusProtocol
 *   2. Lex + Parse
 *   3. Execute
 *   4. Send ResultSet back as formatted string
 *   5. Repeat until client disconnects or sends \quit
 */
public class ClientHandler implements Runnable {

    private final Socket socket;
    private final QueryExecutor executor;
    private final SessionContext session;

    public ClientHandler(Socket socket, QueryExecutor executor) {
        this.socket   = socket;
        this.executor = executor;
        this.session  = new SessionContext(socket);
    }

    @Override
    public void run() {
        System.out.printf("[+] Client connected %s%n", session.getSummary());

        try (
            InputStream  in  = socket.getInputStream();
            OutputStream out = socket.getOutputStream()
        ) {
            // Send welcome banner
            NexusProtocol.writeOK(out,
                "NexusDB v1.0.0 | session=" + session.getSessionId() + "\n");

            // Main request loop
            while (session.isActive()) {
                String sql;
                try {
                    sql = NexusProtocol.readRequest(in);
                } catch (EOFException e) {
                    break; // client disconnected cleanly
                }

                sql = sql.trim();
                if (sql.isEmpty()) continue;

                // Handle built-in commands
                if (sql.equalsIgnoreCase("\\quit") ||
                    sql.equalsIgnoreCase("\\q")) {
                    NexusProtocol.writeResponse(out,
                        NexusProtocol.STATUS_GOODBYE, "Bye!");
                    break;
                }

                if (sql.equalsIgnoreCase("\\ping")) {
                    NexusProtocol.writeOK(out, "PONG");
                    continue;
                }

                if (sql.equalsIgnoreCase("\\status")) {
                    NexusProtocol.writeOK(out, session.getSummary());
                    continue;
                }

                // Execute SQL
                String response = executeSQL(sql);
                session.incrementQueryCount();

                NexusProtocol.writeOK(out, response);
            }

        } catch (IOException e) {
            System.err.printf("[-] Client error %s: %s%n",
                session.getClientAddress(), e.getMessage());
        } finally {
            session.close();
            try { socket.close(); } catch (IOException ignored) {}
            System.out.printf("[-] Client disconnected %s%n", session.getSummary());
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
