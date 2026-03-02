package com.nexusdb.sql.ast;

public class DeleteStatement extends Statement {
    public final String tableName;
    public final WhereClause where;

    public DeleteStatement(String tableName, WhereClause where) {
        this.tableName = tableName;
        this.where = where;
    }

    @Override public Type getType() { return Type.DELETE; }
}
