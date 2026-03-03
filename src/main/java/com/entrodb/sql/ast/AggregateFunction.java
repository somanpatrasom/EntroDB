package com.entrodb.sql.ast;

public class AggregateFunction {

    public enum Type { COUNT, SUM, AVG, MIN, MAX }

    public final Type   type;
    public final String column;  // "*" for COUNT(*)
    public final String alias;   // result column name

    public AggregateFunction(Type type, String column) {
        this.type   = type;
        this.column = column;
        this.alias  = type.name().toLowerCase()
                    + "(" + column + ")";
    }

    @Override public String toString() { return alias; }
}
