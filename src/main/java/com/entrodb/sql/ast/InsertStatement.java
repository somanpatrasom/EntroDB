package com.entrodb.sql.ast;

import java.util.List;

public class InsertStatement extends Statement {
    public final String tableName;
    public final List<String> columnNames; // nullable = INSERT INTO t VALUES(...)
    public final List<String> values;

    public InsertStatement(String tableName, List<String> columnNames, List<String> values) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.values = values;
    }

    @Override public Type getType() { return Type.INSERT; }
}
