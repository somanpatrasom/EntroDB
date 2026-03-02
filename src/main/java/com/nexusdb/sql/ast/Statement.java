package com.nexusdb.sql.ast;

public abstract class Statement {
    public enum Type { SELECT, INSERT, CREATE_TABLE, DROP_TABLE, DELETE, UPDATE, SHOW_TABLES }
    public abstract Type getType();
}
