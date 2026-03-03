package com.entrodb.catalog;

import java.util.*;

public class TableSchema {

    private final String           tableName;
    private final List<Column>     columns;
    private final Map<String, Integer> columnIndex;
    private final List<Constraint> constraints;

    public TableSchema(String tableName, List<Column> columns) {
        this(tableName, columns, new ArrayList<>());
    }

    public TableSchema(String tableName, List<Column> columns,
                       List<Constraint> constraints) {
        this.tableName   = tableName;
        this.columns     = List.copyOf(columns);
        this.constraints = new ArrayList<>(constraints);
        this.columnIndex = new HashMap<>();

        for (int i = 0; i < columns.size(); i++)
            columnIndex.put(columns.get(i).name().toLowerCase(), i);

        // Auto-generate constraints from column definitions
        for (Column col : columns) {
            if (col.primaryKey()) {
                this.constraints.add(new Constraint(Constraint.Type.PRIMARY_KEY, col.name()));
                this.constraints.add(new Constraint(Constraint.Type.NOT_NULL,    col.name()));
                this.constraints.add(new Constraint(Constraint.Type.UNIQUE,      col.name()));
            } else if (!col.nullable()) {
                this.constraints.add(new Constraint(Constraint.Type.NOT_NULL, col.name()));
            }
        }
    }

    // ── Constraint access ─────────────────────────────────────────

    public List<Constraint> getConstraints() {
        return Collections.unmodifiableList(constraints);
    }

    public void addConstraint(Constraint c) {
        constraints.add(c);
    }

    public List<Constraint> getConstraintsForColumn(String colName) {
        List<Constraint> result = new ArrayList<>();
        for (Constraint c : constraints)
            if (c.getColumnName() != null &&
                c.getColumnName().equalsIgnoreCase(colName))
                result.add(c);
        return result;
    }

    public boolean hasPrimaryKey() {
        return constraints.stream()
            .anyMatch(c -> c.getType() == Constraint.Type.PRIMARY_KEY);
    }

    public String getPrimaryKeyColumn() {
        return constraints.stream()
            .filter(c -> c.getType() == Constraint.Type.PRIMARY_KEY)
            .map(Constraint::getColumnName)
            .findFirst().orElse(null);
    }

    // ── Column access ─────────────────────────────────────────────

    public String      getTableName()          { return tableName; }
    public List<Column> getColumns()           { return columns; }

    public int getColumnIndex(String name) {
        Integer idx = columnIndex.get(name.toLowerCase());
        if (idx == null) throw new RuntimeException("Column not found: " + name);
        return idx;
    }

    public Column getColumn(String name) {
        return columns.get(getColumnIndex(name));
    }

    public boolean hasColumn(String name) {
        return columnIndex.containsKey(name.toLowerCase());
    }

    public Column getPrimaryKey() {
        return columns.stream()
            .filter(Column::primaryKey)
            .findFirst().orElse(null);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Table: " + tableName + "\n");
        for (Column col : columns) {
            sb.append("  ").append(col.name())
              .append(" ").append(col.type())
              .append(col.primaryKey() ? " PRIMARY KEY" : "")
              .append(col.nullable()   ? ""             : " NOT NULL")
              .append("\n");
        }
        if (!constraints.isEmpty()) {
            sb.append("Constraints:\n");
            for (Constraint c : constraints)
                sb.append("  ").append(c).append("\n");
        }
        return sb.toString();
    }
}
