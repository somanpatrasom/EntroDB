package com.entrodb.sql.ast;

public class WhereClause {

    public final String              column;
    public final String              operator;
    public final String              value;
    public final SubqueryExpression  subquery; // non-null if subquery WHERE

    // Simple WHERE col OP value
    public WhereClause(String column, String operator, String value) {
        this.column   = column;
        this.operator = operator;
        this.value    = value;
        this.subquery = null;
    }

    // Subquery WHERE
    public WhereClause(SubqueryExpression subquery) {
        this.column   = subquery.column;
        this.operator = subquery.operator;
        this.value    = null;
        this.subquery = subquery;
    }

    public boolean hasSubquery() { return subquery != null; }
}
