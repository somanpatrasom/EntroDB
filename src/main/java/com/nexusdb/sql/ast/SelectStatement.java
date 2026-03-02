package com.nexusdb.sql.ast;

import java.util.List;

public class SelectStatement extends Statement {
    public final List<String> columns;  // ["*"] or ["col1","col2"]
    public final String tableName;
    public final WhereClause where;     // nullable

    public SelectStatement(List<String> columns, String tableName, WhereClause where) {
        this.columns = columns;
        this.tableName = tableName;
        this.where = where;
    }

    @Override public Type getType() { return Type.SELECT; }
}
