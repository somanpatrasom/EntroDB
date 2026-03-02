package com.nexusdb.sql;

import com.nexusdb.catalog.*;
import com.nexusdb.sql.ast.*;
import java.util.*;
import static com.nexusdb.sql.Token.TokenType.*;

public class Parser {

    private final List<Token> tokens;
    private int pos;

    public Parser(List<Token> tokens) {
        this.tokens = tokens;
        this.pos = 0;
    }

    public Statement parse() {
        Token t = peek();
        return switch (t.type()) {
            case SELECT -> parseSelect();
            case INSERT -> parseInsert();
            case CREATE -> parseCreate();
            case DROP   -> parseDrop();
            case DELETE -> parseDelete();
            case UPDATE -> parseUpdate();
            case SHOW   -> parseShow();
            default -> throw new RuntimeException("Unexpected token: " + t);
        };
    }

    // ── SELECT ────────────────────────────────────────────────────

    private SelectStatement parseSelect() {
        consume(SELECT);
        List<String> cols = new ArrayList<>();
        if (peek().type() == STAR) { consume(STAR); cols.add("*"); }
        else {
            cols.add(consume(IDENTIFIER).value());
            while (peek().type() == COMMA) { consume(COMMA); cols.add(consume(IDENTIFIER).value()); }
        }
        consume(FROM);
        String table = consume(IDENTIFIER).value();
        WhereClause where = null;
        if (peek().type() == Token.TokenType.WHERE) where = parseWhere();
        return new SelectStatement(cols, table, where);
    }

    // ── INSERT ────────────────────────────────────────────────────

    private InsertStatement parseInsert() {
        consume(INSERT); consume(INTO);
        String table = consume(IDENTIFIER).value();
        List<String> colNames = null;
        if (peek().type() == LPAREN) {
            colNames = parseIdentifierList();
        }
        consume(VALUES);
        List<String> vals = parseValueList();
        return new InsertStatement(table, colNames, vals);
    }

    private List<String> parseIdentifierList() {
        consume(LPAREN);
        List<String> list = new ArrayList<>();
        list.add(consume(IDENTIFIER).value());
        while (peek().type() == COMMA) { consume(COMMA); list.add(consume(IDENTIFIER).value()); }
        consume(RPAREN);
        return list;
    }

    private List<String> parseValueList() {
        consume(LPAREN);
        List<String> vals = new ArrayList<>();
        vals.add(consumeLiteral());
        while (peek().type() == COMMA) { consume(COMMA); vals.add(consumeLiteral()); }
        consume(RPAREN);
        return vals;
    }

    private String consumeLiteral() {
        Token t = tokens.get(pos++);
        return switch (t.type()) {
            case INTEGER_LITERAL, FLOAT_LITERAL, STRING_LITERAL, BOOL_LITERAL, NULL -> t.value();
            default -> throw new RuntimeException("Expected literal, got: " + t);
        };
    }

    // ── CREATE TABLE ──────────────────────────────────────────────

    private Statement parseCreate() {
        consume(CREATE); consume(TABLE);
        String table = consume(IDENTIFIER).value();
        consume(LPAREN);
        List<Column> cols = new ArrayList<>();
        cols.add(parseColumnDef());
        while (peek().type() == COMMA) { consume(COMMA); cols.add(parseColumnDef()); }
        consume(RPAREN);
        return new CreateTableStatement(table, cols);
    }

    private Column parseColumnDef() {
        String name = consume(IDENTIFIER).value();
        Column.DataType type = parseDataType();
        boolean pk = false, nullable = true;

        while (peek().type() == PRIMARY || peek().type() == Token.TokenType.NOT) {
            if (peek().type() == PRIMARY) {
                consume(PRIMARY); consume(KEY); pk = true; nullable = false;
            } else {
                consume(Token.TokenType.NOT); consume(NULL); nullable = false;
            }
        }
        return new Column(name, type, nullable, pk);
    }

    private Column.DataType parseDataType() {
        return switch (tokens.get(pos++).type()) {
            case INT     -> Column.DataType.INT;
            case BIGINT  -> Column.DataType.BIGINT;
            case FLOAT   -> Column.DataType.FLOAT;
            case BOOLEAN -> Column.DataType.BOOLEAN;
            case VARCHAR -> {
                if (peek().type() == LPAREN) {
                    consume(LPAREN); consume(INTEGER_LITERAL); consume(RPAREN);
                }
                yield Column.DataType.VARCHAR;
            }
            default -> throw new RuntimeException("Unknown type at pos " + (pos - 1));
        };
    }

    // ── DROP TABLE ────────────────────────────────────────────────

    private Statement parseDrop() {
        consume(DROP); consume(TABLE);
        final String table = consume(IDENTIFIER).value();
        return new Statement() {
            public Type getType() { return Type.DROP_TABLE; }
            public final String tableName = table;
        };
    }

    // ── DELETE ────────────────────────────────────────────────────

    private DeleteStatement parseDelete() {
        consume(DELETE); consume(FROM);
        String table = consume(IDENTIFIER).value();
        WhereClause where = null;
        if (peek().type() == Token.TokenType.WHERE) where = parseWhere();
        return new DeleteStatement(table, where);
    }

    // ── UPDATE ────────────────────────────────────────────────────

    private UpdateStatement parseUpdate() {
        consume(UPDATE);
        String table = consume(IDENTIFIER).value();
        consume(SET);

        Map<String, String> setClauses = new LinkedHashMap<>();
        String col = consume(IDENTIFIER).value();
        consume(EQ);
        String val = consumeLiteral();
        setClauses.put(col, val);

        while (peek().type() == COMMA) {
            consume(COMMA);
            String c = consume(IDENTIFIER).value();
            consume(EQ);
            String v = consumeLiteral();
            setClauses.put(c, v);
        }

        WhereClause where = null;
        if (peek().type() == Token.TokenType.WHERE) where = parseWhere();

        return new UpdateStatement(table, setClauses, where);
    }

    // ── SHOW TABLES ───────────────────────────────────────────────

    private Statement parseShow() {
        consume(SHOW); consume(TABLES);
        return new Statement() { public Type getType() { return Type.SHOW_TABLES; } };
    }

    // ── WHERE ─────────────────────────────────────────────────────

    private WhereClause parseWhere() {
        consume(Token.TokenType.WHERE);
        String col = consume(IDENTIFIER).value();
        String op  = consumeOperator();
        String val = consumeLiteral();
        return new WhereClause(col, op, val);
    }

    private String consumeOperator() {
        Token t = tokens.get(pos++);
        return switch (t.type()) {
            case EQ  -> "=";  case NEQ -> "!=";
            case LT  -> "<";  case GT  -> ">";
            case LTE -> "<="; case GTE -> ">=";
            default -> throw new RuntimeException("Expected operator, got: " + t);
        };
    }

    // ── Helpers ───────────────────────────────────────────────────

    private Token peek() { return tokens.get(pos); }

    private Token consume(Token.TokenType expected) {
        Token t = tokens.get(pos++);
        if (t.type() != expected)
            throw new RuntimeException("Expected " + expected + " but got " + t);
        return t;
    }
}
