package com.nexusdb.sql;

public record Token(TokenType type, String value) {

    public enum TokenType {
        // Keywords
        SELECT, FROM, WHERE, INSERT, INTO, VALUES,
        CREATE, TABLE, DROP, DELETE, UPDATE, SET,
        AND, OR, NOT, NULL, PRIMARY, KEY, INT,
        BIGINT, FLOAT, VARCHAR, BOOLEAN, SHOW, TABLES,

        // Symbols
        STAR, COMMA, LPAREN, RPAREN, SEMICOLON,
        EQ, NEQ, LT, GT, LTE, GTE,

        // Literals
        IDENTIFIER, INTEGER_LITERAL, FLOAT_LITERAL,
        STRING_LITERAL, BOOL_LITERAL,

        EOF
    }

    @Override
    public String toString() {
        return type + (value != null ? "('" + value + "')" : "");
    }
}
