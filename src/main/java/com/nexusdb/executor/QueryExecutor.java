package com.nexusdb.executor;

import com.nexusdb.buffer.*;
import com.nexusdb.catalog.*;
import com.nexusdb.index.*;
import com.nexusdb.sql.ast.*;
import com.nexusdb.storage.*;
import com.nexusdb.transaction.*;
import com.nexusdb.util.ByteUtils;
import java.io.IOException;
import java.util.*;

public class QueryExecutor {

    private final BufferPoolManager  bpm;
    private final CatalogManager     catalog;
    private final TransactionManager txnManager;
    private final ConstraintChecker  constraintChecker;
    private final IndexManager       indexManager;

    public QueryExecutor(BufferPoolManager bpm, CatalogManager catalog,
                         TransactionManager txnManager, IndexManager indexManager) {
        this.bpm               = bpm;
        this.catalog           = catalog;
        this.txnManager        = txnManager;
        this.constraintChecker = new ConstraintChecker(bpm);
        this.indexManager      = indexManager;
    }

    public ResultSet execute(Statement stmt) throws IOException {
        return switch (stmt.getType()) {
            case SELECT       -> executeSelect((SelectStatement) stmt);
            case INSERT       -> executeInsert((InsertStatement) stmt);
            case CREATE_TABLE -> executeCreate((CreateTableStatement) stmt);
            case DROP_TABLE   -> executeDrop(stmt);
            case DELETE       -> executeDelete((DeleteStatement) stmt);
            case UPDATE       -> executeUpdate((UpdateStatement) stmt);
            case SHOW_TABLES  -> executeShowTables();
        };
    }

    // ── CREATE TABLE ──────────────────────────────────────────────

    private ResultSet executeCreate(CreateTableStatement stmt) throws IOException {
        TableSchema schema = new TableSchema(stmt.tableName, stmt.columns);
        catalog.createTable(schema);
        bpm.getDiskManager().createTable(stmt.tableName.toLowerCase());
        bpm.newPage(stmt.tableName.toLowerCase());
        indexManager.initTable(schema); // create PK index
        return new ResultSet("Table '" + stmt.tableName + "' created.");
    }

    // ── DROP TABLE ────────────────────────────────────────────────

    private ResultSet executeDrop(Statement stmt) throws IOException {
        String tableName;
        try {
            var f = stmt.getClass().getField("tableName");
            tableName = (String) f.get(stmt);
        } catch (Exception e) { throw new RuntimeException(e); }
        catalog.dropTable(tableName);
        bpm.getDiskManager().dropTable(tableName.toLowerCase());
        indexManager.dropTableIndexes(tableName);
        return new ResultSet("Table '" + tableName + "' dropped.");
    }

    // ── INSERT ────────────────────────────────────────────────────

    private ResultSet executeInsert(InsertStatement stmt) throws IOException {
        TableSchema  schema  = catalog.getTable(stmt.tableName);
        String       tableId = stmt.tableName.toLowerCase();
        List<Object> values  = coerceValues(stmt.values, schema, stmt.columnNames);

        constraintChecker.checkInsert(values, schema);

        byte[] record = ByteUtils.serializeRow(values, schema);

        Transaction txn = txnManager.begin();
        try {
            int pageCount = bpm.getDiskManager().getPageCount(tableId);
            for (int i = 0; i < pageCount; i++) {
                PageId pid  = new PageId(tableId, i);
                Page   page = bpm.fetchPage(pid);
                int    slot = page.insertRecord(record);
                if (slot >= 0) {
                    long lsn = txnManager.logInsert(txn, tableId, i, slot, record);
                    page.setPageLSN(lsn);
                    bpm.unpinPage(pid, true);
                    RID rid = new RID(i, slot);
                    indexManager.onInsert(schema, values, rid);
                    txnManager.commit(txn);
                    return new ResultSet("1 row inserted (page=" + i + ", slot=" + slot + ")");
                }
                bpm.unpinPage(pid, false);
            }

            Page   newPage = bpm.newPage(tableId);
            int    slot    = newPage.insertRecord(record);
            PageId pid     = newPage.getPageId();
            long   lsn     = txnManager.logInsert(txn, tableId, pid.pageNumber(), slot, record);
            newPage.setPageLSN(lsn);
            bpm.unpinPage(pid, true);
            RID rid = new RID(pid.pageNumber(), slot);
            indexManager.onInsert(schema, values, rid);
            txnManager.commit(txn);
            return new ResultSet("1 row inserted (page=" + pid.pageNumber()
                + ", slot=" + slot + ")");

        } catch (Exception e) {
            txnManager.abort(txn);
            throw e;
        }
    }

    // ── SELECT ────────────────────────────────────────────────────

    private ResultSet executeSelect(SelectStatement stmt) throws IOException {
        TableSchema schema  = catalog.getTable(stmt.tableName);
        String      tableId = stmt.tableName.toLowerCase();

        boolean      selectAll  = stmt.columns.size() == 1 && stmt.columns.get(0).equals("*");
        List<String> resultCols = selectAll
            ? schema.getColumns().stream().map(Column::name).toList()
            : stmt.columns;

        // ── Index-accelerated lookup ──────────────────────────────
        if (stmt.where != null && stmt.where.operator.equals("=")) {
            String pkCol = schema.getPrimaryKeyColumn();
            if (pkCol != null && pkCol.equalsIgnoreCase(stmt.where.column)) {
                return indexedSelect(schema, tableId, stmt, resultCols, selectAll);
            }
        }

        // ── Full scan fallback ────────────────────────────────────
        return fullScanSelect(schema, tableId, stmt, resultCols, selectAll);
    }

    private ResultSet indexedSelect(TableSchema schema, String tableId,
                                     SelectStatement stmt,
                                     List<String> resultCols,
                                     boolean selectAll) throws IOException {
        Column pkCol  = schema.getColumn(stmt.where.column);
        Object lookupVal = parseValue(stmt.where.value, pkCol.type());

        RID rid = indexManager.lookup(schema.getTableName(),
                                       stmt.where.column, lookupVal);
        if (rid == null) return new ResultSet(resultCols, new ArrayList<>());

        PageId pid  = new PageId(tableId, rid.pageNum());
        Page   page = bpm.fetchPage(pid);
        byte[] record = page.readRecord(rid.slot());
        bpm.unpinPage(pid, false);

        if (record == null) return new ResultSet(resultCols, new ArrayList<>());

        List<Object>       fullRow = ByteUtils.deserializeRow(record, schema);
        List<List<Object>> rows    = new ArrayList<>();

        if (selectAll) {
            rows.add(fullRow);
        } else {
            List<Object> projected = new ArrayList<>();
            for (String col : stmt.columns)
                projected.add(fullRow.get(schema.getColumnIndex(col)));
            rows.add(projected);
        }
        return new ResultSet(resultCols, rows);
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
                byte[] record = page.readRecord(s);
                if (record == null) continue;
                List<Object> fullRow = ByteUtils.deserializeRow(record, schema);
                if (stmt.where != null && !matchesWhere(fullRow, stmt.where, schema)) continue;

                if (selectAll) {
                    rows.add(fullRow);
                } else {
                    List<Object> projected = new ArrayList<>();
                    for (String col : stmt.columns)
                        projected.add(fullRow.get(schema.getColumnIndex(col)));
                    rows.add(projected);
                }
            }
            bpm.unpinPage(pid, false);
        }
        return new ResultSet(resultCols, rows);
    }

    // ── UPDATE ────────────────────────────────────────────────────

    private ResultSet executeUpdate(UpdateStatement stmt) throws IOException {
        TableSchema schema    = catalog.getTable(stmt.tableName);
        String      tableId   = stmt.tableName.toLowerCase();
        int         updated   = 0;
        int         pageCount = bpm.getDiskManager().getPageCount(tableId);

        Transaction txn = txnManager.begin();
        try {
            for (int p = 0; p < pageCount; p++) {
                PageId  pid      = new PageId(tableId, p);
                Page    page     = bpm.fetchPage(pid);
                boolean modified = false;
                int     numSlots = page.getNumSlots();

                for (int s = 0; s < numSlots; s++) {
                    byte[] before = page.readRecord(s);
                    if (before == null) continue;

                    List<Object> oldRow = ByteUtils.deserializeRow(before, schema);
                    if (stmt.where != null && !matchesWhere(oldRow, stmt.where, schema)) continue;

                    List<Object> newRow = new ArrayList<>(oldRow);
                    for (Map.Entry<String, String> entry : stmt.setClauses.entrySet()) {
                        int    colIdx = schema.getColumnIndex(entry.getKey());
                        Column col    = schema.getColumns().get(colIdx);
                        Object newVal = "null".equalsIgnoreCase(entry.getValue())
                            ? null : parseValue(entry.getValue(), col.type());
                        newRow.set(colIdx, newVal);
                    }

                    constraintChecker.checkUpdate(newRow, schema, p, s);

                    byte[] after = ByteUtils.serializeRow(newRow, schema);
                    long   lsn   = txnManager.logUpdate(txn, tableId, p, s, before, after);

                    boolean inPlace = page.updateRecord(s, after);
                    RID newRid;
                    if (inPlace) {
                        page.setPageLSN(lsn);
                        newRid = new RID(p, s);
                    } else {
                        page.deleteRecord(s);
                        bpm.unpinPage(pid, true);
                        newRid = insertIntoAnyPage(tableId, after, lsn, pageCount);
                        page = bpm.fetchPage(pid);
                    }

                    indexManager.onUpdate(schema, oldRow, newRow, newRid);
                    modified = true;
                    updated++;
                }
                bpm.unpinPage(pid, modified);
            }

            txnManager.commit(txn);
            return new ResultSet(updated + " row(s) updated.");

        } catch (Exception e) {
            txnManager.abort(txn);
            throw e;
        }
    }

    // ── DELETE ────────────────────────────────────────────────────

    private ResultSet executeDelete(DeleteStatement stmt) throws IOException {
        TableSchema schema  = catalog.getTable(stmt.tableName);
        String      tableId = stmt.tableName.toLowerCase();
        int         deleted = 0;

        Transaction txn = txnManager.begin();
        try {
            int pageCount = bpm.getDiskManager().getPageCount(tableId);
            for (int p = 0; p < pageCount; p++) {
                PageId  pid      = new PageId(tableId, p);
                Page    page     = bpm.fetchPage(pid);
                boolean modified = false;

                for (int s = 0; s < page.getNumSlots(); s++) {
                    byte[] record = page.readRecord(s);
                    if (record == null) continue;
                    List<Object> row = ByteUtils.deserializeRow(record, schema);
                    if (stmt.where == null || matchesWhere(row, stmt.where, schema)) {
                        long lsn = txnManager.logDelete(txn, tableId, p, s, record);
                        page.deleteRecord(s);
                        page.setPageLSN(lsn);
                        indexManager.onDelete(schema, row);
                        deleted++;
                        modified = true;
                    }
                }
                bpm.unpinPage(pid, modified);
            }

            txnManager.commit(txn);
            return new ResultSet(deleted + " row(s) deleted.");

        } catch (Exception e) {
            txnManager.abort(txn);
            throw e;
        }
    }

    // ── SHOW TABLES ───────────────────────────────────────────────

    private ResultSet executeShowTables() {
        List<List<Object>> rows = new ArrayList<>();
        for (TableSchema s : catalog.getAllTables())
            rows.add(List.of(s.getTableName(), s.getColumns().size() + " columns"));
        return new ResultSet(List.of("table_name", "info"), rows);
    }

    // ── Helpers ───────────────────────────────────────────────────

    private RID insertIntoAnyPage(String tableId, byte[] record,
                                   long lsn, int pageCount) throws IOException {
        for (int pp = 0; pp < pageCount; pp++) {
            PageId ppid  = new PageId(tableId, pp);
            Page   ppage = bpm.fetchPage(ppid);
            int    slot  = ppage.insertRecord(record);
            if (slot >= 0) {
                ppage.setPageLSN(lsn);
                bpm.unpinPage(ppid, true);
                return new RID(pp, slot);
            }
            bpm.unpinPage(ppid, false);
        }
        Page overflow = bpm.newPage(tableId);
        int  slot     = overflow.insertRecord(record);
        overflow.setPageLSN(lsn);
        PageId pid = overflow.getPageId();
        bpm.unpinPage(pid, true);
        return new RID(pid.pageNumber(), slot);
    }

    private boolean matchesWhere(List<Object> row, WhereClause where, TableSchema schema) {
        int    idx = schema.getColumnIndex(where.column);
        Object val = row.get(idx);
        if (val == null) return false;
        Column col        = schema.getColumns().get(idx);
        Object compareVal = parseValue(where.value, col.type());
        int    cmp        = compareObjects(val, compareVal);
        return switch (where.operator) {
            case "="  -> cmp == 0;  case "!=" -> cmp != 0;
            case "<"  -> cmp < 0;   case ">"  -> cmp > 0;
            case "<=" -> cmp <= 0;  case ">=" -> cmp >= 0;
            default   -> false;
        };
    }

    @SuppressWarnings("unchecked")
    private int compareObjects(Object a, Object b) {
        if (a instanceof Comparable ca) return ca.compareTo(b);
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

    private List<Object> coerceValues(List<String> rawVals, TableSchema schema,
                                       List<String> colNames) {
        List<Column> cols   = schema.getColumns();
        List<Object> result = new ArrayList<>(Collections.nCopies(cols.size(), null));
        if (colNames == null) {
            if (rawVals.size() != cols.size())
                throw new RuntimeException("Value count mismatch: expected " + cols.size());
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
