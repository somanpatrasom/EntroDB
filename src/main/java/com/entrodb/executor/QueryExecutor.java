package com.entrodb.executor;

import com.entrodb.buffer.*;
import com.entrodb.catalog.*;
import com.entrodb.index.*;
import com.entrodb.sql.ast.*;
import com.entrodb.storage.*;
import com.entrodb.transaction.*;
import com.entrodb.util.ByteUtils;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import com.entrodb.sql.ast.SubqueryExpression;
import java.util.*;

public class QueryExecutor {

    private final BufferPoolManager  bpm;
    private final CatalogManager     catalog;
    private final TransactionManager txnManager;
    private final ConstraintChecker  constraintChecker;
    private final IndexManager       indexManager;

    public QueryExecutor(BufferPoolManager bpm, CatalogManager catalog,
                         TransactionManager txnManager,
                         IndexManager indexManager) {
        this.bpm               = bpm;
        this.catalog           = catalog;
        this.txnManager        = txnManager;
        this.constraintChecker = new ConstraintChecker(bpm);
        this.indexManager      = indexManager;
    }

    public ResultSet execute(Statement stmt) throws IOException {
        try {
            return switch (stmt.getType()) {
                case SELECT       -> executeSelect((SelectStatement) stmt);
                case INSERT       -> executeInsert((InsertStatement) stmt);
                case CREATE_TABLE -> executeCreate((CreateTableStatement) stmt);
                case DROP_TABLE   -> executeDrop(stmt);
                case DELETE       -> executeDelete((DeleteStatement) stmt);
                case UPDATE       -> executeUpdate((UpdateStatement) stmt);
                case SHOW_TABLES  -> executeShowTables();
                case CREATE_INDEX -> executeCreateIndex((CreateIndexStatement) stmt);
                case DROP_INDEX   -> executeDropIndex((DropIndexStatement) stmt);
                case SHOW_INDEXES -> executeShowIndexes((ShowIndexesStatement) stmt);
            };
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Query interrupted: " + e.getMessage());
        }
    }

    private ResultSet executeCreate(CreateTableStatement stmt)
            throws IOException, InterruptedException {
        TableSchema schema = new TableSchema(stmt.tableName, stmt.columns);
        catalog.createTable(schema);
        bpm.getDiskManager().createTable(stmt.tableName.toLowerCase());
        bpm.newPage(stmt.tableName.toLowerCase());
        indexManager.initTable(schema);
        return new ResultSet("Table '" + stmt.tableName + "' created.");
    }

    private ResultSet executeDrop(Statement stmt)
            throws IOException, InterruptedException {
        String tableName;
        try { var f = stmt.getClass().getField("tableName");
              tableName = (String) f.get(stmt);
        } catch (Exception e) { throw new RuntimeException(e); }
        Transaction txn = txnManager.begin();
        try {
            txnManager.lockTableWrite(txn, tableName.toLowerCase());
            catalog.dropTable(tableName);
            bpm.getDiskManager().dropTable(tableName.toLowerCase());
            indexManager.dropTableIndexes(tableName);
            txnManager.commit(txn);
            return new ResultSet("Table '" + tableName + "' dropped.");
        } catch (Exception e) { txnManager.abort(txn); throw e; }
    }

    private ResultSet executeCreateIndex(CreateIndexStatement stmt)
            throws IOException, InterruptedException {
        TableSchema schema = catalog.getTable(stmt.tableName);
        indexManager.createIndex(stmt.indexName, stmt.tableName,
            stmt.columnName, stmt.unique, schema, bpm);
        return new ResultSet("Index '" + stmt.indexName + "' created on "
            + stmt.tableName + "." + stmt.columnName + ".");
    }

    private ResultSet executeDropIndex(DropIndexStatement stmt)
            throws IOException, InterruptedException {
        indexManager.dropIndex(stmt.indexName, stmt.tableName);
        return new ResultSet("Index '" + stmt.indexName + "' dropped.");
    }

    private ResultSet executeShowIndexes(ShowIndexesStatement stmt) {
        List<List<Object>> rows = new ArrayList<>();
        for (String[] info : indexManager.getIndexesForTable(stmt.tableName))
            rows.add(List.of(info[0], info[1], info[2], info[3]));
        return new ResultSet(
            List.of("index_name", "table", "column", "entries"), rows);
    }

    private ResultSet executeInsert(InsertStatement stmt)
            throws IOException, InterruptedException {
        TableSchema  schema  = catalog.getTable(stmt.tableName);
        String       tableId = stmt.tableName.toLowerCase();
        List<Object> values  = coerceValues(stmt.values, schema, stmt.columnNames);
        constraintChecker.checkInsert(values, schema);
        byte[] record = ByteUtils.serializeRow(values, schema);

        Transaction txn = txnManager.begin();
        try {
            txnManager.lockTableWrite(txn, tableId);
            int pageCount = bpm.getDiskManager().getPageCount(tableId);
            for (int i = 0; i < pageCount; i++) {
                PageId pid  = new PageId(tableId, i);
                Page   page = bpm.fetchPage(pid);
                int    slot = page.insertRecord(record);
                if (slot >= 0) {
                    long lsn = txnManager.logInsert(txn, tableId, i, slot, record);
                    page.setPageLSN(lsn);
                    bpm.unpinPage(pid, true);
                    indexManager.onInsert(schema, values, new RID(i, slot));
                    txnManager.commit(txn);
                    return new ResultSet("1 row inserted (page=" + i
                        + ", slot=" + slot + ")");
                }
                bpm.unpinPage(pid, false);
            }
            Page   newPage = bpm.newPage(tableId);
            int    slot    = newPage.insertRecord(record);
            PageId pid     = newPage.getPageId();
            long   lsn     = txnManager.logInsert(txn, tableId,
                                 pid.pageNumber(), slot, record);
            newPage.setPageLSN(lsn);
            bpm.unpinPage(pid, true);
            indexManager.onInsert(schema, values, new RID(pid.pageNumber(), slot));
            txnManager.commit(txn);
            return new ResultSet("1 row inserted (page=" + pid.pageNumber()
                + ", slot=" + slot + ")");
        } catch (Exception e) { txnManager.abort(txn); throw e; }
    }

    private ResultSet executeSelect(SelectStatement stmt)
            throws IOException, InterruptedException {
        if (stmt.join != null)          return executeJoin(stmt);
        if (stmt.hasAggregates())       return executeAggregate(stmt);

        TableSchema schema    = catalog.getTable(stmt.tableName);
        String      tableId   = stmt.tableName.toLowerCase();
        boolean     selectAll = stmt.columns.size() == 1
                              && stmt.columns.get(0).equals("*");
        List<String> resultCols = selectAll
            ? schema.getColumns().stream().map(Column::name).toList()
            : stripTablePrefix(stmt.columns);

        Transaction txn = txnManager.begin();
        try {
            txnManager.lockTableRead(txn, tableId);
            ResultSet result;
            if (stmt.where != null && !stmt.where.hasSubquery()
                    && stmt.where.operator.equals("=")
                    && indexManager.hasIndex(schema.getTableName(),
                                              stmt.where.column)) {
                result = indexedSelect(schema, tableId, stmt, resultCols, selectAll);
            } else {
                result = fullScanSelect(schema, tableId, stmt, resultCols, selectAll);
            }
            txnManager.commit(txn);
            return result;
        } catch (Exception e) { txnManager.abort(txn); throw e; }
    }

    private ResultSet indexedSelect(TableSchema schema, String tableId,
                                     SelectStatement stmt,
                                     List<String> resultCols,
                                     boolean selectAll) throws IOException {
        Column col       = schema.getColumn(stmt.where.column);
        Object lookupVal = parseValue(stmt.where.value, col.type());
        RID    rid       = indexManager.lookup(schema.getTableName(),
                               stmt.where.column, lookupVal);
        if (rid == null) return new ResultSet(resultCols, new ArrayList<>());
        PageId pid    = new PageId(tableId, rid.pageNum());
        Page   page   = bpm.fetchPage(pid);
        byte[] record = page.readRecord(rid.slot());
        bpm.unpinPage(pid, false);
        if (record == null) return new ResultSet(resultCols, new ArrayList<>());
        List<Object> fullRow = ByteUtils.deserializeRow(record, schema);
        return new ResultSet(resultCols,
            List.of(selectAll ? fullRow : project(fullRow, stmt.columns, schema)));
    }

    private ResultSet fullScanSelect(TableSchema schema, String tableId,
                                      SelectStatement stmt,
                                      List<String> resultCols,
                                      boolean selectAll) throws IOException {
        List<List<Object>> rows      = new ArrayList<>();
        int                pageCount = bpm.getDiskManager().getPageCount(tableId);
        for (int p = 0; p < pageCount; p++) {
            PageId pid  = new PageId(tableId, p);
            Page   page = bpm.fetchPage(pid);
            for (int s = 0; s < page.getNumSlots(); s++) {
                byte[] rec = page.readRecord(s);
                if (rec == null) continue;
                List<Object> row = ByteUtils.deserializeRow(rec, schema);
                if (stmt.where != null
                        && !matchesWhere(row, stmt.where, schema)) continue;
                rows.add(selectAll ? row : project(row, stmt.columns, schema));
            }
            bpm.unpinPage(pid, false);
        }
        return new ResultSet(resultCols, rows);
    }

    // ── AGGREGATE ─────────────────────────────────────────────────

    private ResultSet executeAggregate(SelectStatement stmt)
            throws IOException, InterruptedException {
        TableSchema schema  = catalog.getTable(stmt.tableName);
        String      tableId = stmt.tableName.toLowerCase();

        Transaction txn = txnManager.begin();
        try {
            txnManager.lockTableRead(txn, tableId);
            List<List<Object>> allRows   = new ArrayList<>();
            int                pageCount = bpm.getDiskManager().getPageCount(tableId);
            for (int p = 0; p < pageCount; p++) {
                PageId pid  = new PageId(tableId, p);
                Page   page = bpm.fetchPage(pid);
                for (int s = 0; s < page.getNumSlots(); s++) {
                    byte[] rec = page.readRecord(s);
                    if (rec == null) continue;
                    List<Object> row = ByteUtils.deserializeRow(rec, schema);
                    if (stmt.where == null
                            || matchesWhere(row, stmt.where, schema))
                        allRows.add(row);
                }
                bpm.unpinPage(pid, false);
            }
            txnManager.commit(txn);
            return stmt.groupBy != null
                ? computeGroupBy(allRows, stmt, schema)
                : computeGlobalAggregate(allRows, stmt, schema);
        } catch (Exception e) { txnManager.abort(txn); throw e; }
    }

    private ResultSet computeGlobalAggregate(List<List<Object>> rows,
                                               SelectStatement stmt,
                                               TableSchema schema) {
        List<String> cols = new ArrayList<>();
        List<Object> row  = new ArrayList<>();
        for (AggregateFunction agg : stmt.aggregates) {
            cols.add(agg.alias);
            row.add(computeAggregate(agg, rows, schema));
        }
        return new ResultSet(cols, List.of(row));
    }

    private ResultSet computeGroupBy(List<List<Object>> rows,
                                      SelectStatement stmt,
                                      TableSchema schema) {
        int groupColIdx = schema.getColumnIndex(stmt.groupBy);
        LinkedHashMap<Object, List<List<Object>>> groups = new LinkedHashMap<>();
        for (List<Object> row : rows)
            groups.computeIfAbsent(row.get(groupColIdx),
                k -> new ArrayList<>()).add(row);

        List<String> resultCols = new ArrayList<>();
        resultCols.add(stmt.groupBy);
        stmt.aggregates.forEach(agg -> resultCols.add(agg.alias));

        List<List<Object>> resultRows = new ArrayList<>();
        for (Map.Entry<Object, List<List<Object>>> e : groups.entrySet()) {
            List<Object> resultRow = new ArrayList<>();
            resultRow.add(e.getKey());
            stmt.aggregates.forEach(agg ->
                resultRow.add(computeAggregate(agg, e.getValue(), schema)));
            resultRows.add(resultRow);
        }
        return new ResultSet(resultCols, resultRows);
    }

    private Object computeAggregate(AggregateFunction agg,
                                     List<List<Object>> rows,
                                     TableSchema schema) {
        return switch (agg.type) {
            case COUNT -> (long) rows.size();
            case SUM -> {
                if (agg.column.equals("*")) yield (long) rows.size();
                int ci = schema.getColumnIndex(agg.column);
                // Check runtime type of first non-null value
                boolean useInt = false;
                for (List<Object> r : rows) {
                    Object v = r.get(ci);
                    if (v != null) { useInt = (v instanceof Integer || v instanceof Long); break; }
                }
                if (useInt) {
                    long lsum = 0L;
                    for (List<Object> r : rows) {
                        Object v = r.get(ci);
                        if (v != null) lsum += ((Number) v).longValue();
                    }
                    yield lsum;
                } else {
                    double dsum = 0.0;
                    for (List<Object> r : rows) {
                        Object v = r.get(ci);
                        if (v != null) dsum += ((Number) v).doubleValue();
                    }
                    yield dsum;
                }
            }
            case AVG -> {
                int ci = schema.getColumnIndex(agg.column);
                OptionalDouble avg = rows.stream().map(r -> r.get(ci))
                    .filter(Objects::nonNull)
                    .mapToDouble(v -> ((Number) v).doubleValue()).average();
                yield avg.isPresent()
                    ? Math.round(avg.getAsDouble() * 100.0) / 100.0 : null;
            }
            case MIN -> {
                int    ci  = schema.getColumnIndex(agg.column);
                Object min = null;
                for (List<Object> r : rows) {
                    Object v = r.get(ci);
                    if (v != null && (min == null || compareObjects(v, min) < 0))
                        min = v;
                }
                yield min;
            }
            case MAX -> {
                int    ci  = schema.getColumnIndex(agg.column);
                Object max = null;
                for (List<Object> r : rows) {
                    Object v = r.get(ci);
                    if (v != null && (max == null || compareObjects(v, max) > 0))
                        max = v;
                }
                yield max;
            }
        };
    }

    private boolean isIntCol(TableSchema schema, String col) {
        if (col == null || col.equals("*")) return false;
        try {
            Column.DataType t = schema.getColumn(col).type();
            return t == Column.DataType.INT || t == Column.DataType.BIGINT;
        } catch (Exception e) { return false; }
    }

    // ── JOIN ──────────────────────────────────────────────────────

    private ResultSet executeJoin(SelectStatement stmt)
            throws IOException, InterruptedException {
        TableSchema leftSchema  = catalog.getTable(stmt.tableName);
        TableSchema rightSchema = catalog.getTable(stmt.join.rightTable);
        JoinClause  join        = stmt.join;
        String leftTableId  = stmt.tableName.toLowerCase();
        String rightTableId = stmt.join.rightTable.toLowerCase();
        List<String> resultCols = buildJoinColumns(stmt.columns,
            leftSchema, rightSchema, stmt.tableName, stmt.join.rightTable);
        boolean useIndex = indexManager.hasIndex(
            rightSchema.getTableName(), join.rightCol);

        Transaction txn = txnManager.begin();
        try {
            txnManager.lockTableRead(txn, leftTableId);
            txnManager.lockTableRead(txn, rightTableId);
            List<List<Object>> rows = new ArrayList<>();
            int leftPageCount = bpm.getDiskManager().getPageCount(leftTableId);
            for (int p = 0; p < leftPageCount; p++) {
                PageId leftPid  = new PageId(leftTableId, p);
                Page   leftPage = bpm.fetchPage(leftPid);
                for (int s = 0; s < leftPage.getNumSlots(); s++) {
                    byte[] leftRec = leftPage.readRecord(s);
                    if (leftRec == null) continue;
                    List<Object> leftRow =
                        ByteUtils.deserializeRow(leftRec, leftSchema);
                    if (stmt.where != null
                            && !stmt.where.hasSubquery()
                            && leftSchema.hasColumn(stmt.where.column)
                            && !matchesWhere(leftRow, stmt.where, leftSchema))
                        continue;
                    Object leftJoinVal =
                        leftRow.get(leftSchema.getColumnIndex(join.leftCol));
                    List<List<Object>> matchingRight = useIndex
                        ? probeIndex(rightSchema, rightTableId,
                                      join.rightCol, leftJoinVal)
                        : scanRight(rightSchema, rightTableId,
                                     join.rightCol, leftJoinVal);
                    if (matchingRight.isEmpty()) {
                        if (join.type == JoinType.LEFT) {
                            List<Object> combined = new ArrayList<>(leftRow);
                            for (int i = 0; i < rightSchema.getColumns().size(); i++)
                                combined.add(null);
                            rows.add(projectJoin(combined, stmt.columns,
                                leftSchema, rightSchema,
                                stmt.tableName, stmt.join.rightTable));
                        }
                    } else {
                        for (List<Object> rightRow : matchingRight) {
                            if (stmt.where != null
                                    && !stmt.where.hasSubquery()
                                    && rightSchema.hasColumn(stmt.where.column)
                                    && !matchesWhere(rightRow, stmt.where, rightSchema))
                                continue;
                            List<Object> combined = new ArrayList<>(leftRow);
                            combined.addAll(rightRow);
                            rows.add(projectJoin(combined, stmt.columns,
                                leftSchema, rightSchema,
                                stmt.tableName, stmt.join.rightTable));
                        }
                    }
                }
                bpm.unpinPage(leftPid, false);
            }
            txnManager.commit(txn);
            return new ResultSet(resultCols, rows);
        } catch (Exception e) { txnManager.abort(txn); throw e; }
    }

    private List<List<Object>> probeIndex(TableSchema rightSchema,
                                            String rightTableId,
                                            String rightCol,
                                            Object joinVal) throws IOException {
        if (joinVal == null) return Collections.emptyList();
        Column col = rightSchema.getColumn(rightCol);
        RID    rid = indexManager.lookup(rightSchema.getTableName(), rightCol,
                         coerceToType(joinVal, col.type()));
        if (rid == null) return Collections.emptyList();
        PageId pid    = new PageId(rightTableId, rid.pageNum());
        Page   page   = bpm.fetchPage(pid);
        byte[] record = page.readRecord(rid.slot());
        bpm.unpinPage(pid, false);
        if (record == null) return Collections.emptyList();
        return List.of(ByteUtils.deserializeRow(record, rightSchema));
    }

    private List<List<Object>> scanRight(TableSchema rightSchema,
                                          String rightTableId,
                                          String rightCol,
                                          Object joinVal) throws IOException {
        List<List<Object>> matches = new ArrayList<>();
        int pageCount = bpm.getDiskManager().getPageCount(rightTableId);
        int colIdx    = rightSchema.getColumnIndex(rightCol);
        for (int p = 0; p < pageCount; p++) {
            PageId pid  = new PageId(rightTableId, p);
            Page   page = bpm.fetchPage(pid);
            for (int s = 0; s < page.getNumSlots(); s++) {
                byte[] rec = page.readRecord(s);
                if (rec == null) continue;
                List<Object> row = ByteUtils.deserializeRow(rec, rightSchema);
                Object val = row.get(colIdx);
                if (joinVal == null ? val == null : joinVal.equals(val))
                    matches.add(row);
            }
            bpm.unpinPage(pid, false);
        }
        return matches;
    }

    // ── UPDATE ────────────────────────────────────────────────────

    private ResultSet executeUpdate(UpdateStatement stmt)
            throws IOException, InterruptedException {
        TableSchema schema    = catalog.getTable(stmt.tableName);
        String      tableId   = stmt.tableName.toLowerCase();
        int         updated   = 0;
        int         pageCount = bpm.getDiskManager().getPageCount(tableId);
        Transaction txn = txnManager.begin();
        try {
            txnManager.lockTableWrite(txn, tableId);
            for (int p = 0; p < pageCount; p++) {
                PageId  pid      = new PageId(tableId, p);
                Page    page     = bpm.fetchPage(pid);
                boolean modified = false;
                int     numSlots = page.getNumSlots();
                for (int s = 0; s < numSlots; s++) {
                    byte[] before = page.readRecord(s);
                    if (before == null) continue;
                    List<Object> oldRow = ByteUtils.deserializeRow(before, schema);
                    if (stmt.where != null
                            && !matchesWhere(oldRow, stmt.where, schema)) continue;
                    txnManager.lockRowWrite(txn, tableId, p, s);
                    List<Object> newRow = new ArrayList<>(oldRow);
                    for (Map.Entry<String, String> e : stmt.setClauses.entrySet()) {
                        int    ci  = schema.getColumnIndex(e.getKey());
                        Column col = schema.getColumns().get(ci);
                        newRow.set(ci, "null".equalsIgnoreCase(e.getValue())
                            ? null : parseValue(e.getValue(), col.type()));
                    }
                    constraintChecker.checkUpdate(newRow, schema, p, s);
                    byte[] after = ByteUtils.serializeRow(newRow, schema);
                    long   lsn   = txnManager.logUpdate(txn, tableId,
                                       p, s, before, after);
                    boolean inPlace = page.updateRecord(s, after);
                    RID newRid;
                    if (inPlace) { page.setPageLSN(lsn); newRid = new RID(p, s); }
                    else {
                        page.deleteRecord(s); bpm.unpinPage(pid, true);
                        newRid = insertIntoAnyPage(tableId, after, lsn, pageCount);
                        page   = bpm.fetchPage(pid);
                    }
                    indexManager.onUpdate(schema, oldRow, newRow, newRid);
                    modified = true; updated++;
                }
                bpm.unpinPage(pid, modified);
            }
            txnManager.commit(txn);
            return new ResultSet(updated + " row(s) updated.");
        } catch (Exception e) { txnManager.abort(txn); throw e; }
    }

    // ── DELETE ────────────────────────────────────────────────────

    private ResultSet executeDelete(DeleteStatement stmt)
            throws IOException, InterruptedException {
        TableSchema schema  = catalog.getTable(stmt.tableName);
        String      tableId = stmt.tableName.toLowerCase();
        int         deleted = 0;
        Transaction txn = txnManager.begin();
        try {
            txnManager.lockTableWrite(txn, tableId);
            int pageCount = bpm.getDiskManager().getPageCount(tableId);
            for (int p = 0; p < pageCount; p++) {
                PageId  pid      = new PageId(tableId, p);
                Page    page     = bpm.fetchPage(pid);
                boolean modified = false;
                for (int s = 0; s < page.getNumSlots(); s++) {
                    byte[] rec = page.readRecord(s);
                    if (rec == null) continue;
                    List<Object> row = ByteUtils.deserializeRow(rec, schema);
                    if (stmt.where == null
                            || matchesWhere(row, stmt.where, schema)) {
                        txnManager.lockRowWrite(txn, tableId, p, s);
                        long lsn = txnManager.logDelete(txn, tableId, p, s, rec);
                        page.deleteRecord(s); page.setPageLSN(lsn);
                        indexManager.onDelete(schema, row);
                        deleted++; modified = true;
                    }
                }
                bpm.unpinPage(pid, modified);
            }
            txnManager.commit(txn);
            return new ResultSet(deleted + " row(s) deleted.");
        } catch (Exception e) { txnManager.abort(txn); throw e; }
    }

    // ── SHOW TABLES ───────────────────────────────────────────────

    private ResultSet executeShowTables() {
        List<List<Object>> rows = new ArrayList<>();
        for (TableSchema s : catalog.getAllTables())
            rows.add(List.of(s.getTableName(),
                s.getColumns().size() + " columns"));
        return new ResultSet(List.of("table_name", "info"), rows);
    }

    // ── Helpers ───────────────────────────────────────────────────

    private List<String> buildJoinColumns(List<String> req,
                                           TableSchema left, TableSchema right,
                                           String leftName, String rightName) {
        if (req.size() == 1 && req.get(0).equals("*")) {
            List<String> cols = new ArrayList<>();
            left.getColumns().forEach(c -> cols.add(leftName + "." + c.name()));
            right.getColumns().forEach(c -> cols.add(rightName + "." + c.name()));
            return cols;
        }
        return new ArrayList<>(req);
    }

    private List<Object> projectJoin(List<Object> combined, List<String> req,
                                      TableSchema left, TableSchema right,
                                      String leftName, String rightName) {
        if (req.size() == 1 && req.get(0).equals("*")) return combined;
        List<Object> result = new ArrayList<>();
        int leftSize = left.getColumns().size();
        for (String col : req) {
            if (col.contains(".")) {
                String[] p = col.split("\\.", 2);
                result.add(p[0].equalsIgnoreCase(leftName)
                    ? combined.get(left.getColumnIndex(p[1]))
                    : combined.get(leftSize + right.getColumnIndex(p[1])));
            } else {
                result.add(left.hasColumn(col)
                    ? combined.get(left.getColumnIndex(col))
                    : combined.get(leftSize + right.getColumnIndex(col)));
            }
        }
        return result;
    }

    private List<Object> project(List<Object> row, List<String> cols,
                                  TableSchema schema) {
        List<Object> result = new ArrayList<>();
        for (String c : cols) {
            String name = c.contains(".") ? c.split("\\.")[1] : c;
            result.add(row.get(schema.getColumnIndex(name)));
        }
        return result;
    }

    private List<String> stripTablePrefix(List<String> cols) {
        List<String> r = new ArrayList<>();
        for (String c : cols)
            r.add(c.contains(".") ? c.split("\\.")[1] : c);
        return r;
    }

    private RID insertIntoAnyPage(String tableId, byte[] record,
                                   long lsn, int pageCount) throws IOException {
        for (int pp = 0; pp < pageCount; pp++) {
            PageId ppid  = new PageId(tableId, pp);
            Page   ppage = bpm.fetchPage(ppid);
            int    slot  = ppage.insertRecord(record);
            if (slot >= 0) {
                ppage.setPageLSN(lsn); bpm.unpinPage(ppid, true);
                return new RID(pp, slot);
            }
            bpm.unpinPage(ppid, false);
        }
        Page overflow = bpm.newPage(tableId);
        int  slot = overflow.insertRecord(record);
        overflow.setPageLSN(lsn);
        PageId pid = overflow.getPageId();
        bpm.unpinPage(pid, true);
        return new RID(pid.pageNumber(), slot);
    }

    private boolean matchesWhere(List<Object> row, WhereClause where,
                                  TableSchema schema) throws IOException {
        // Subquery WHERE
        if (where.hasSubquery()) {
            return matchesSubquery(row, where.subquery, schema);
        }
        int    idx = schema.getColumnIndex(where.column);
        Object val = row.get(idx);
        if (val == null) return false;
        Object cmp = parseValue(where.value,
            schema.getColumns().get(idx).type());
        int c = compareObjects(val, cmp);
        return switch (where.operator) {
            case "="  -> c == 0;  case "!=" -> c != 0;
            case "<"  -> c < 0;   case ">"  -> c > 0;
            case "<=" -> c <= 0;  case ">=" -> c >= 0;
            default   -> false;
        };
    }

    private boolean matchesSubquery(List<Object> row,
                                     SubqueryExpression sub,
                                     TableSchema schema)
            throws IOException {
        switch (sub.type) {

            case IN, NOT_IN -> {
                // Execute subquery to get a set of values
                SelectStatement subStmt = sub.subquery;
                Set<Object> values = executeSubqueryAsSet(subStmt);
                int colIdx = schema.getColumnIndex(sub.column);
                Object val = row.get(colIdx);
                boolean inSet = values.contains(val);
                return sub.type == SubqueryExpression.Type.IN ? inSet : !inSet;
            }

            case EXISTS -> {
                // EXISTS — true if subquery returns any rows
                SelectStatement subStmt = sub.subquery;
                ResultSet result = executeSelectInternal(subStmt);
                return !result.getRows().isEmpty();
            }

            case SCALAR -> {
                // col OP (SELECT single value)
                SelectStatement subStmt = sub.subquery;
                ResultSet result = executeSelectInternal(subStmt);
                if (result.getRows().isEmpty()) return false;
                Object scalarVal = result.getRows().get(0).get(0);
                if (scalarVal == null) return false;
                int colIdx = schema.getColumnIndex(sub.column);
                Object val = row.get(colIdx);
                if (val == null) return false;
                int c = compareObjects(val, scalarVal);
                return switch (sub.operator) {
                    case "="  -> c == 0;  case "!=" -> c != 0;
                    case "<"  -> c < 0;   case ">"  -> c > 0;
                    case "<=" -> c <= 0;  case ">=" -> c >= 0;
                    default   -> false;
                };
            }

            default -> { return false; }
        }
    }

    private Set<Object> executeSubqueryAsSet(SelectStatement stmt)
            throws IOException {
        ResultSet result = executeSelectInternal(stmt);
        Set<Object> values = new HashSet<>();
        for (List<Object> r : result.getRows())
            if (!r.isEmpty() && r.get(0) != null)
                values.add(r.get(0));
        return values;
    }

    private ResultSet executeSelectInternal(SelectStatement stmt)
            throws IOException {
        try {
            return executeSelect(stmt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Subquery interrupted");
        }
    }

    @SuppressWarnings("unchecked")
    private int compareObjects(Object a, Object b) {
        // Handle mixed numeric types (e.g. Integer vs Double from scalar subquery)
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a instanceof Comparable ca) {
            try { return ca.compareTo(b); }
            catch (ClassCastException e) {
                return a.toString().compareTo(b.toString());
            }
        }
        return a.toString().compareTo(b.toString());
    }

    private Object parseValue(String raw, Column.DataType type) {
        return switch (type) {
            case INT     -> Integer.parseInt(raw);
            case BIGINT  -> Long.parseLong(raw);
            case FLOAT   -> Double.parseDouble(raw);
            case BOOLEAN -> Boolean.parseBoolean(raw);
            case VARCHAR -> raw;
        };
    }

    private Object coerceToType(Object val, Column.DataType type) {
        if (val == null) return null;
        return switch (type) {
            case INT     -> val instanceof Integer ? val
                            : Integer.parseInt(val.toString());
            case BIGINT  -> val instanceof Long ? val
                            : Long.parseLong(val.toString());
            case FLOAT   -> val instanceof Double ? val
                            : Double.parseDouble(val.toString());
            case BOOLEAN -> val instanceof Boolean ? val
                            : Boolean.parseBoolean(val.toString());
            case VARCHAR -> val.toString();
        };
    }

    private List<Object> coerceValues(List<String> rawVals,
                                       TableSchema schema,
                                       List<String> colNames) {
        List<Column> cols   = schema.getColumns();
        List<Object> result = new ArrayList<>(
            Collections.nCopies(cols.size(), null));
        if (colNames == null) {
            if (rawVals.size() != cols.size())
                throw new RuntimeException(
                    "Value count mismatch: expected " + cols.size());
            for (int i = 0; i < cols.size(); i++)
                result.set(i, "null".equalsIgnoreCase(rawVals.get(i))
                    ? null : parseValue(rawVals.get(i), cols.get(i).type()));
        } else {
            for (int i = 0; i < colNames.size(); i++) {
                int idx = schema.getColumnIndex(colNames.get(i));
                result.set(idx, "null".equalsIgnoreCase(rawVals.get(i))
                    ? null : parseValue(rawVals.get(i), cols.get(idx).type()));
            }
        }
        return result;
    }
}
