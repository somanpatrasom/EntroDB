package com.nexusdb.executor;

import java.util.*;

public class ResultSet {
    private final List<String> columns;
    private final List<List<Object>> rows;
    private final String message; // for DDL statements

    public ResultSet(List<String> columns, List<List<Object>> rows) {
        this.columns = columns;
        this.rows = rows;
        this.message = null;
    }

    public ResultSet(String message) {
        this.columns = Collections.emptyList();
        this.rows = Collections.emptyList();
        this.message = message;
    }

    public List<String> getColumns() { return columns; }
    public List<List<Object>> getRows() { return rows; }
    public boolean isMessage() { return message != null; }
    public String getMessage() { return message; }

    public String prettyPrint() {
        if (isMessage()) return message;
        if (columns.isEmpty()) return "(empty)";

        // Calculate column widths
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) widths[i] = columns.get(i).length();
        for (List<Object> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                String s = row.get(i) == null ? "NULL" : row.get(i).toString();
                widths[i] = Math.max(widths[i], s.length());
            }
        }

        StringBuilder sb = new StringBuilder();
        String separator = buildSeparator(widths);
        sb.append(separator);
        sb.append(buildRow(columns.stream().map(c -> (Object)c).toList(), widths));
        sb.append(separator);
        for (List<Object> row : rows) sb.append(buildRow(row, widths));
        sb.append(separator);
        sb.append(rows.size()).append(" row(s)\n");
        return sb.toString();
    }

    private String buildSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int w : widths) sb.append("-".repeat(w + 2)).append("+");
        return sb.append("\n").toString();
    }

    private String buildRow(List<Object> row, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < row.size(); i++) {
            String val = row.get(i) == null ? "NULL" : row.get(i).toString();
            sb.append(" ").append(val).append(" ".repeat(widths[i] - val.length())).append(" |");
        }
        return sb.append("\n").toString();
    }
}
