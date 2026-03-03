package com.entrodb.sql.ast;

public class WhereClause {
    public final String column;
    public final String operator; // =, !=, <, >, <=, >=
    public final String value;

    public WhereClause(String column, String operator, String value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
}
