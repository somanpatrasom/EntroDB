package com.nexusdb.catalog;

/**
 * Thrown when an integrity constraint is violated.
 * Carries enough detail for a clear error message.
 */
public class ConstraintViolationException extends RuntimeException {

    private final Constraint.Type constraintType;
    private final String          tableName;
    private final String          columnName;
    private final Object          offendingValue;

    public ConstraintViolationException(Constraint.Type type, String table,
                                         String column, Object value) {
        super(buildMessage(type, table, column, value));
        this.constraintType = type;
        this.tableName      = table;
        this.columnName     = column;
        this.offendingValue = value;
    }

    private static String buildMessage(Constraint.Type type, String table,
                                        String column, Object value) {
        return switch (type) {
            case PRIMARY_KEY -> String.format(
                "PRIMARY KEY violation on table '%s': " +
                "duplicate value '%s' in column '%s'", table, value, column);
            case NOT_NULL -> String.format(
                "NOT NULL violation on table '%s': " +
                "column '%s' cannot be null", table, column);
            case UNIQUE -> String.format(
                "UNIQUE violation on table '%s': " +
                "duplicate value '%s' in column '%s'", table, value, column);
            case CHECK -> String.format(
                "CHECK violation on table '%s': " +
                "value '%s' in column '%s' failed check constraint",
                table, value, column);
        };
    }

    public Constraint.Type getConstraintType() { return constraintType; }
    public String          getTableName()      { return tableName; }
    public String          getColumnName()     { return columnName; }
    public Object          getOffendingValue() { return offendingValue; }
}
