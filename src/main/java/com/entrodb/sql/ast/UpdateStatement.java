package com.entrodb.sql.ast;

import java.util.Map;

public class UpdateStatement extends Statement {
    public final String tableName;
    public final Map<String, String> setClauses; // col → raw value
    public final WhereClause where;              // nullable

    public UpdateStatement(String tableName, Map<String, String> setClauses, WhereClause where) {
        this.tableName  = tableName;
        this.setClauses = setClauses;
        this.where      = where;
    }

    @Override public Type getType() { return Type.UPDATE; }
}
