package com.nexusdb.catalog;

/**
 * Represents an integrity constraint on a table.
 *
 * Types:
 *   PRIMARY_KEY — one per table, implies NOT NULL + UNIQUE
 *   NOT_NULL    — column cannot be null
 *   UNIQUE      — all values in column must be distinct
 *   CHECK       — simple expression: col OP value (e.g. age > 0)
 */
public class Constraint {

    public enum Type { PRIMARY_KEY, NOT_NULL, UNIQUE, CHECK }

    private final Type   type;
    private final String columnName;
    private final String checkExpression; // only for CHECK constraints e.g. "age > 0"

    public Constraint(Type type, String columnName) {
        this(type, columnName, null);
    }

    public Constraint(Type type, String columnName, String checkExpression) {
        this.type            = type;
        this.columnName      = columnName;
        this.checkExpression = checkExpression;
    }

    public Type   getType()            { return type; }
    public String getColumnName()      { return columnName; }
    public String getCheckExpression() { return checkExpression; }

    @Override
    public String toString() {
        return switch (type) {
            case PRIMARY_KEY -> "PRIMARY KEY (" + columnName + ")";
            case NOT_NULL    -> "NOT NULL (" + columnName + ")";
            case UNIQUE      -> "UNIQUE (" + columnName + ")";
            case CHECK       -> "CHECK (" + checkExpression + ")";
        };
    }

    // Serialize to string for catalog persistence
    public String serialize() {
        return type + ":" + columnName + ":" +
            (checkExpression != null ? checkExpression : "");
    }

    public static Constraint deserialize(String s) {
        String[] parts = s.split(":", 3);
        Type type = Type.valueOf(parts[0]);
        String col = parts[1];
        String expr = parts[2].isEmpty() ? null : parts[2];
        return new Constraint(type, col, expr);
    }
}
