package com.entrodb.sql.ast;

public class DropIndexStatement extends Statement {
    public final String indexName;
    public final String tableName;

    public DropIndexStatement(String indexName, String tableName) {
        this.indexName = indexName;
        this.tableName = tableName;
    }

    @Override public Type getType() { return Type.DROP_INDEX; }
}
