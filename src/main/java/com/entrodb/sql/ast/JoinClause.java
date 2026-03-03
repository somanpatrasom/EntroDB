package com.entrodb.sql.ast;

public class JoinClause {
    public final JoinType type;
    public final String   rightTable;
    public final String   leftCol;   // e.g. "users.id"
    public final String   rightCol;  // e.g. "orders.user_id"

    public JoinClause(JoinType type, String rightTable,
                      String leftCol, String rightCol) {
        this.type       = type;
        this.rightTable = rightTable;
        this.leftCol    = leftCol;
        this.rightCol   = rightCol;
    }
}
