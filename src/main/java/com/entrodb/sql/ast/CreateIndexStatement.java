package com.entrodb.sql.ast;

public class CreateIndexStatement extends Statement {
    public final String indexName;
    public final String tableName;
    public final String columnName;
    public final boolean unique;

    public CreateIndexStatement(String indexName, String tableName,
                                 String columnName, boolean unique) {
        this.indexName  = indexName;
        this.tableName  = tableName;
        this.columnName = columnName;
        this.unique     = unique;
    }

    @Override public Type getType() { return Type.CREATE_INDEX; }
}
