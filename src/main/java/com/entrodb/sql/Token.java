package com.entrodb.sql;

public record Token(TokenType type, String value) {

    public enum TokenType {
        SELECT, FROM, WHERE, INSERT, INTO, VALUES,
        CREATE, TABLE, DROP, DELETE, UPDATE, SET,
        AND, OR, NOT, NULL, PRIMARY, KEY, INT,
        BIGINT, FLOAT, VARCHAR, BOOLEAN, SHOW, TABLES,
        INDEX, ON, UNIQUE, INDEXES,
        JOIN, INNER, LEFT, OUTER,
        COUNT, SUM, AVG, MIN, MAX,
        GROUP, BY, HAVING, AS,

        STAR, COMMA, LPAREN, RPAREN, SEMICOLON, DOT,
        EQ, NEQ, LT, GT, LTE, GTE,

        IDENTIFIER, INTEGER_LITERAL, FLOAT_LITERAL,
        STRING_LITERAL, BOOL_LITERAL,

        EOF
    }

    @Override
    public String toString() {
        return type + (value != null ? "('" + value + "')" : "");
    }
}
