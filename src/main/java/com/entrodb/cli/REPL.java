package com.entrodb.cli;

import com.entrodb.executor.*;
import com.entrodb.sql.*;
import com.entrodb.sql.ast.Statement;
import java.util.Scanner;

/**
 * Interactive REPL — the user-facing command line interface.
 * Type SQL queries ending with ; to execute.
 * Type \q to quit, \help for help.
 */
public class REPL {

    private final QueryExecutor executor;

    public REPL(QueryExecutor executor) {
        this.executor = executor;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("╔══════════════════════════════╗");
        System.out.println("║       EntroDB v1.0.0         ║");
        System.out.println("║  Type \\help for commands     ║");
        System.out.println("╚══════════════════════════════╝");

        StringBuilder inputBuffer = new StringBuilder();

        while (true) {
            System.out.print(inputBuffer.length() == 0 ? "nexus> " : "   ... ");
            String line = scanner.nextLine().trim();

            if (line.equalsIgnoreCase("\\q") || line.equalsIgnoreCase("\\quit")) {
                System.out.println("Goodbye.");
                break;
            }
            if (line.equalsIgnoreCase("\\help")) {
                printHelp();
                continue;
            }

            inputBuffer.append(" ").append(line);

            if (line.endsWith(";")) {
                String sql = inputBuffer.toString().trim();
                inputBuffer.setLength(0);
                executeSQL(sql);
            }
        }
    }

    private void executeSQL(String sql) {
        long start = System.currentTimeMillis();
        try {
            Lexer lexer = new Lexer(sql);
            Parser parser = new Parser(lexer.tokenize());
            Statement stmt = parser.parse();
            ResultSet result = executor.execute(stmt);
            System.out.print(result.prettyPrint());
            System.out.printf("Time: %.3fms%n", (System.currentTimeMillis() - start) * 1.0);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private void printHelp() {
        System.out.println("""
            EntroDB Commands:
              CREATE TABLE name (col TYPE [PRIMARY KEY] [NOT NULL], ...);
              INSERT INTO name VALUES (v1, v2, ...);
              INSERT INTO name (col1, col2) VALUES (v1, v2);
              SELECT * FROM name [WHERE col op val];
              SELECT col1, col2 FROM name [WHERE col op val];
              DELETE FROM name [WHERE col op val];
              DROP TABLE name;
              SHOW TABLES;
              \\q    — quit
              \\help — this message
            Types: INT, BIGINT, FLOAT, VARCHAR(n), BOOLEAN
            """);
    }
}
