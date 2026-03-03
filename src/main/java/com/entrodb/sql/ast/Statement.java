package com.entrodb.sql.ast;

public abstract class Statement {
    public enum Type {
        SELECT, INSERT, CREATE_TABLE, DROP_TABLE,
        DELETE, UPDATE, SHOW_TABLES,
        CREATE_INDEX, DROP_INDEX, SHOW_INDEXES
    }
    public abstract Type getType();
}
