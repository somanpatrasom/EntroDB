package com.nexusdb.sql;

import java.util.*;
import static com.nexusdb.sql.Token.TokenType.*;

/**
 * Tokenizes a SQL string into a list of Tokens.
 */
public class Lexer {

    private final String input;
    private int pos;
    private final List<Token> tokens = new ArrayList<>();

    private static final Map<String, Token.TokenType> KEYWORDS = Map.ofEntries(
        Map.entry("select", SELECT), Map.entry("from", FROM),
        Map.entry("where", WHERE), Map.entry("insert", INSERT),
        Map.entry("into", INTO), Map.entry("values", VALUES),
        Map.entry("create", CREATE), Map.entry("table", TABLE),
        Map.entry("drop", DROP), Map.entry("delete", DELETE),
        Map.entry("update", UPDATE), Map.entry("set", SET),
        Map.entry("and", AND), Map.entry("or", OR),
        Map.entry("not", NOT), Map.entry("null", NULL),
        Map.entry("primary", PRIMARY), Map.entry("key", KEY),
        Map.entry("int", INT), Map.entry("bigint", BIGINT),
        Map.entry("float", FLOAT), Map.entry("varchar", VARCHAR),
        Map.entry("boolean", BOOLEAN), Map.entry("show", SHOW),
        Map.entry("tables", TABLES),
        Map.entry("true", BOOL_LITERAL), Map.entry("false", BOOL_LITERAL)
    );

    public Lexer(String input) {
        this.input = input.trim();
        this.pos = 0;
    }

    public List<Token> tokenize() {
        while (pos < input.length()) {
            skipWhitespace();
            if (pos >= input.length()) break;

            char c = input.charAt(pos);

            if (Character.isLetter(c) || c == '_') readWord();
            else if (Character.isDigit(c)) readNumber();
            else if (c == '\'') readString();
            else readSymbol();
        }
        tokens.add(new Token(EOF, null));
        return tokens;
    }

    private void readWord() {
        int start = pos;
        while (pos < input.length() && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_'))
            pos++;
        String word = input.substring(start, pos);
        Token.TokenType type = KEYWORDS.getOrDefault(word.toLowerCase(), IDENTIFIER);
        tokens.add(new Token(type, word));
    }

    private void readNumber() {
        int start = pos;
        boolean isFloat = false;
        while (pos < input.length() && (Character.isDigit(input.charAt(pos)) || input.charAt(pos) == '.')) {
            if (input.charAt(pos) == '.') isFloat = true;
            pos++;
        }
        tokens.add(new Token(isFloat ? FLOAT_LITERAL : INTEGER_LITERAL, input.substring(start, pos)));
    }

    private void readString() {
        pos++; // skip opening '
        int start = pos;
        while (pos < input.length() && input.charAt(pos) != '\'') pos++;
        tokens.add(new Token(STRING_LITERAL, input.substring(start, pos)));
        pos++; // skip closing '
    }

    private void readSymbol() {
        char c = input.charAt(pos++);
        switch (c) {
            case '*' -> tokens.add(new Token(STAR, "*"));
            case ',' -> tokens.add(new Token(COMMA, ","));
            case '(' -> tokens.add(new Token(LPAREN, "("));
            case ')' -> tokens.add(new Token(RPAREN, ")"));
            case ';' -> tokens.add(new Token(SEMICOLON, ";"));
            case '=' -> tokens.add(new Token(EQ, "="));
            case '<' -> {
                if (pos < input.length() && input.charAt(pos) == '=') { pos++; tokens.add(new Token(LTE, "<=")); }
                else tokens.add(new Token(LT, "<"));
            }
            case '>' -> {
                if (pos < input.length() && input.charAt(pos) == '=') { pos++; tokens.add(new Token(GTE, ">=")); }
                else tokens.add(new Token(GT, ">"));
            }
            case '!' -> {
                if (pos < input.length() && input.charAt(pos) == '=') { pos++; tokens.add(new Token(NEQ, "!=")); }
            }
            default -> {} // skip unknown chars
        }
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) pos++;
    }
}
