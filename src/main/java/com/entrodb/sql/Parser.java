package com.entrodb.sql;

import com.entrodb.catalog.*;
import com.entrodb.sql.ast.*;
import java.util.*;
import static com.entrodb.sql.Token.TokenType.*;

public class Parser {

    private final List<Token> tokens;
    private int pos;

    public Parser(List<Token> tokens) { this.tokens = tokens; this.pos = 0; }

    public Statement parse() {
        return switch (peek().type()) {
            case SELECT -> parseSelect();
            case INSERT -> parseInsert();
            case CREATE -> parseCreate();
            case DROP   -> parseDrop();
            case DELETE -> parseDelete();
            case UPDATE -> parseUpdate();
            case SHOW   -> parseShow();
            default -> throw new RuntimeException("Unexpected token: " + peek());
        };
    }

    // ── SELECT ────────────────────────────────────────────────────

    private SelectStatement parseSelect() {
        consume(SELECT);

        List<AggregateFunction> aggs      = new ArrayList<>();
        List<String>            plainCols = new ArrayList<>();

        if (peek().type() == STAR) {
            consume(STAR); plainCols.add("*");
        } else {
            parseSelectItem(aggs, plainCols);
            while (peek().type() == COMMA) {
                consume(COMMA);
                parseSelectItem(aggs, plainCols);
            }
        }

        consume(FROM);
        String table = consume(IDENTIFIER).value();

        JoinClause join = null;
        if (peek().type() == INNER || peek().type() == LEFT
                || peek().type() == JOIN)
            join = parseJoin();

        WhereClause where = null;
        if (peek().type() == WHERE) where = parseWhere();

        String groupBy = null;
        if (peek().type() == GROUP) {
            consume(GROUP); consume(BY);
            groupBy = consume(IDENTIFIER).value();
        }

        if (!aggs.isEmpty())
            return new SelectStatement(aggs, plainCols, table,
                                        join, where, groupBy);
        return new SelectStatement(plainCols, table, join, where);
    }

    private void parseSelectItem(List<AggregateFunction> aggs,
                                   List<String> cols) {
        if (isAggregateToken(peek().type())) {
            AggregateFunction.Type type = switch (tokens.get(pos++).type()) {
                case COUNT -> AggregateFunction.Type.COUNT;
                case SUM   -> AggregateFunction.Type.SUM;
                case AVG   -> AggregateFunction.Type.AVG;
                case MIN   -> AggregateFunction.Type.MIN;
                case MAX   -> AggregateFunction.Type.MAX;
                default    -> throw new RuntimeException("Expected aggregate");
            };
            consume(LPAREN);
            String col = peek().type() == STAR
                ? consume(STAR).value()
                : consume(IDENTIFIER).value();
            consume(RPAREN);
            if (peek().type() == AS) { consume(AS); consume(IDENTIFIER); }
            aggs.add(new AggregateFunction(type, col));
        } else {
            cols.add(parseColRef());
        }
    }

    private boolean isAggregateToken(Token.TokenType t) {
        return t == COUNT || t == SUM || t == AVG || t == MIN || t == MAX;
    }

    private String parseColRef() {
        String first = consume(IDENTIFIER).value();
        if (peek().type() == DOT) {
            consume(DOT);
            return first + "." + consume(IDENTIFIER).value();
        }
        return first;
    }

    private JoinClause parseJoin() {
        JoinType type;
        if (peek().type() == INNER)     { consume(INNER); consume(JOIN); type = JoinType.INNER; }
        else if (peek().type() == LEFT) { consume(LEFT);
            if (peek().type() == OUTER) consume(OUTER);
            consume(JOIN); type = JoinType.LEFT; }
        else                            { consume(JOIN);  type = JoinType.INNER; }
        String rightTable = consume(IDENTIFIER).value();
        consume(ON);
        String leftRef  = parseColRef(); consume(EQ);
        String rightRef = parseColRef();
        String leftCol  = leftRef.contains(".")  ? leftRef.split("\\.")[1]  : leftRef;
        String rightCol = rightRef.contains(".") ? rightRef.split("\\.")[1] : rightRef;
        return new JoinClause(type, rightTable, leftCol, rightCol);
    }

    // ── INSERT ────────────────────────────────────────────────────

    private InsertStatement parseInsert() {
        consume(INSERT); consume(INTO);
        String table = consume(IDENTIFIER).value();
        List<String> colNames = null;
        if (peek().type() == LPAREN) colNames = parseIdentifierList();
        consume(VALUES);
        return new InsertStatement(table, colNames, parseValueList());
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
            case INTEGER_LITERAL, FLOAT_LITERAL,
                 STRING_LITERAL, BOOL_LITERAL, NULL -> t.value();
            default -> throw new RuntimeException("Expected literal, got: " + t);
        };
    }

    // ── CREATE ────────────────────────────────────────────────────

    private Statement parseCreate() {
        consume(CREATE);
        if (peek().type() == UNIQUE) { consume(UNIQUE); consume(INDEX); return parseCreateIndexBody(true); }
        if (peek().type() == INDEX)  { consume(INDEX);  return parseCreateIndexBody(false); }
        consume(TABLE);
        String table = consume(IDENTIFIER).value();
        consume(LPAREN);
        List<Column> cols = new ArrayList<>();
        cols.add(parseColumnDef());
        while (peek().type() == COMMA) { consume(COMMA); cols.add(parseColumnDef()); }
        consume(RPAREN);
        return new CreateTableStatement(table, cols);
    }

    private CreateIndexStatement parseCreateIndexBody(boolean unique) {
        String idx = consume(IDENTIFIER).value(); consume(ON);
        String table = consume(IDENTIFIER).value();
        consume(LPAREN); String col = consume(IDENTIFIER).value(); consume(RPAREN);
        return new CreateIndexStatement(idx, table, col, unique);
    }

    private Column parseColumnDef() {
        String name = consume(IDENTIFIER).value();
        Column.DataType type = parseDataType();
        boolean pk = false, nullable = true;
        while (peek().type() == PRIMARY || peek().type() == NOT) {
            if (peek().type() == PRIMARY) { consume(PRIMARY); consume(KEY); pk = true; nullable = false; }
            else { consume(NOT); consume(NULL); nullable = false; }
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
            default -> throw new RuntimeException("Unknown type at pos " + (pos-1));
        };
    }

    // ── DROP ──────────────────────────────────────────────────────

    private Statement parseDrop() {
        consume(DROP);
        if (peek().type() == INDEX) {
            consume(INDEX);
            String idx = consume(IDENTIFIER).value(); consume(ON);
            String table = consume(IDENTIFIER).value();
            return new DropIndexStatement(idx, table);
        }
        consume(TABLE);
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
        if (peek().type() == WHERE) where = parseWhere();
        return new DeleteStatement(table, where);
    }

    // ── UPDATE ────────────────────────────────────────────────────

    private UpdateStatement parseUpdate() {
        consume(UPDATE); String table = consume(IDENTIFIER).value(); consume(SET);
        Map<String, String> set = new LinkedHashMap<>();
        set.put(consume(IDENTIFIER).value(), readEqLiteral());
        while (peek().type() == COMMA) { consume(COMMA); set.put(consume(IDENTIFIER).value(), readEqLiteral()); }
        WhereClause where = null;
        if (peek().type() == WHERE) where = parseWhere();
        return new UpdateStatement(table, set, where);
    }

    private String readEqLiteral() { consume(EQ); return consumeLiteral(); }

    // ── SHOW ──────────────────────────────────────────────────────

    private Statement parseShow() {
        consume(SHOW);
        if (peek().type() == INDEXES) {
            consume(INDEXES); consume(ON);
            return new ShowIndexesStatement(consume(IDENTIFIER).value());
        }
        consume(TABLES);
        return new Statement() { public Type getType() { return Type.SHOW_TABLES; } };
    }

    // ── WHERE ─────────────────────────────────────────────────────

    private WhereClause parseWhere() {
        consume(WHERE);
        String colRef = parseColRef();
        String col = colRef.contains(".") ? colRef.split("\\.")[1] : colRef;
        return new WhereClause(col, consumeOperator(), consumeLiteral());
    }

    private String consumeOperator() {
        return switch (tokens.get(pos++).type()) {
            case EQ -> "="; case NEQ -> "!=";
            case LT -> "<"; case GT  -> ">";
            case LTE -> "<="; case GTE -> ">=";
            default -> throw new RuntimeException(
                "Expected operator, got: " + tokens.get(pos-1));
        };
    }

    private Token peek()                            { return tokens.get(pos); }
    private Token consume(Token.TokenType expected) {
        Token t = tokens.get(pos++);
        if (t.type() != expected)
            throw new RuntimeException(
                "Expected " + expected + " but got " + t);
        return t;
    }
}
