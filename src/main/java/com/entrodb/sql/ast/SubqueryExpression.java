package com.entrodb.sql.ast;

public class SubqueryExpression {

    public enum Type {
        IN,         // col IN (SELECT ...)
        NOT_IN,     // col NOT IN (SELECT ...)
        EXISTS,     // EXISTS (SELECT ...)
        SCALAR      // col OP (SELECT single value)
    }

    public final Type             type;
    public final String           column;    // left-hand column (null for EXISTS)
    public final String           operator;  // for SCALAR: =, >, <, etc.
    public final SelectStatement  subquery;

    public SubqueryExpression(Type type, String column,
                               String operator, SelectStatement subquery) {
        this.type     = type;
        this.column   = column;
        this.operator = operator;
        this.subquery = subquery;
    }
}
