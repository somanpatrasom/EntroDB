package com.entrodb.sql.ast;

import java.util.List;

public class SelectStatement extends Statement {

    public final List<String>            columns;
    public final List<AggregateFunction> aggregates;  // non-null if aggregates present
    public final String                  tableName;
    public final JoinClause              join;
    public final WhereClause             where;
    public final String                  groupBy;     // column name or null

    // Plain SELECT (no aggregates)
    public SelectStatement(List<String> columns, String tableName,
                            JoinClause join, WhereClause where) {
        this.columns    = columns;
        this.aggregates = null;
        this.tableName  = tableName;
        this.join       = join;
        this.where      = where;
        this.groupBy    = null;
    }

    // Aggregate SELECT
    public SelectStatement(List<AggregateFunction> aggregates,
                            List<String> groupCols,
                            String tableName,
                            JoinClause join,
                            WhereClause where,
                            String groupBy) {
        this.columns    = groupCols;  // GROUP BY columns to include in output
        this.aggregates = aggregates;
        this.tableName  = tableName;
        this.join       = join;
        this.where      = where;
        this.groupBy    = groupBy;
    }

    public boolean hasAggregates() {
        return aggregates != null && !aggregates.isEmpty();
    }

    @Override public Type getType() { return Type.SELECT; }
}
