package com.entrodb.catalog;

import com.entrodb.buffer.BufferPoolManager;
import com.entrodb.storage.*;
import com.entrodb.util.ByteUtils;

import java.io.IOException;
import java.util.List;

/**
 * Validates all constraints before a write operation.
 *
 * Called by QueryExecutor before every INSERT and UPDATE.
 * Throws ConstraintViolationException immediately on first violation.
 */
public class ConstraintChecker {

    private final BufferPoolManager bpm;

    public ConstraintChecker(BufferPoolManager bpm) {
        this.bpm = bpm;
    }

    /**
     * Validate a row before INSERT.
     * @param values   the full row values in column order
     * @param schema   the table schema
     * @param excludeSlot  -1 normally; for UPDATE, the slot being replaced (skip it)
     */
    public void checkInsert(List<Object> values, TableSchema schema)
            throws IOException {
        checkRow(values, schema, -1, -1);
    }

    /**
     * Validate a row before UPDATE.
     * excludePage/excludeSlot = the original record being replaced (skip it for UNIQUE check).
     */
    public void checkUpdate(List<Object> values, TableSchema schema,
                             int excludePage, int excludeSlot)
            throws IOException {
        checkRow(values, schema, excludePage, excludeSlot);
    }

    private void checkRow(List<Object> values, TableSchema schema,
                           int excludePage, int excludeSlot)
            throws IOException {

        String tableId = schema.getTableName().toLowerCase();

        for (Constraint c : schema.getConstraints()) {
            if (c.getColumnName() == null) continue;

            int    colIdx = schema.getColumnIndex(c.getColumnName());
            Object value  = values.get(colIdx);
            Column col    = schema.getColumns().get(colIdx);

            switch (c.getType()) {

                case NOT_NULL -> {
                    if (value == null)
                        throw new ConstraintViolationException(
                            Constraint.Type.NOT_NULL,
                            schema.getTableName(), c.getColumnName(), null);
                }

                case PRIMARY_KEY, UNIQUE -> {
                    if (value == null) continue; // null allowed for UNIQUE (not PK, handled above)
                    if (isDuplicate(tableId, schema, colIdx, value,
                                    excludePage, excludeSlot))
                        throw new ConstraintViolationException(
                            c.getType(), schema.getTableName(),
                            c.getColumnName(), value);
                }

                case CHECK -> {
                    if (c.getCheckExpression() != null && value != null)
                        if (!evaluateCheck(value, col, c.getCheckExpression()))
                            throw new ConstraintViolationException(
                                Constraint.Type.CHECK,
                                schema.getTableName(), c.getColumnName(), value);
                }
            }
        }
    }

    // ── Duplicate check (full scan — B+ Tree will replace this) ──

    private boolean isDuplicate(String tableId, TableSchema schema,
                                  int colIdx, Object value,
                                  int excludePage, int excludeSlot)
            throws IOException {

        int pageCount = bpm.getDiskManager().getPageCount(tableId);

        for (int p = 0; p < pageCount; p++) {
            PageId pid  = new PageId(tableId, p);
            Page   page = bpm.fetchPage(pid);

            for (int s = 0; s < page.getNumSlots(); s++) {
                // Skip the record being updated
                if (p == excludePage && s == excludeSlot) continue;

                byte[] record = page.readRecord(s);
                if (record == null) continue;

                List<Object> row = ByteUtils.deserializeRow(record, schema);
                Object       existing = row.get(colIdx);

                if (value.equals(existing)) {
                    bpm.unpinPage(pid, false);
                    return true;
                }
            }
            bpm.unpinPage(pid, false);
        }
        return false;
    }

    // ── CHECK expression evaluator ────────────────────────────────

    /**
     * Evaluates simple CHECK expressions: "col OP literal"
     * Supported operators: >, <, >=, <=, =, !=
     * Example: "age > 0", "salary >= 1000"
     */
    private boolean evaluateCheck(Object value, Column col, String expr) {
        // Parse: find operator
        String[] ops = {">=", "<=", "!=", ">", "<", "="};
        for (String op : ops) {
            int idx = expr.indexOf(op);
            if (idx < 0) continue;

            String rhs = expr.substring(idx + op.length()).trim();
            Object rhsVal;
            try {
                rhsVal = switch (col.type()) {
                    case INT     -> Integer.parseInt(rhs);
                    case BIGINT  -> Long.parseLong(rhs);
                    case FLOAT   -> Double.parseDouble(rhs);
                    case VARCHAR -> rhs.replace("'", "");
                    case BOOLEAN -> Boolean.parseBoolean(rhs);
                };
            } catch (NumberFormatException e) {
                return true; // can't parse — skip check
            }

            int cmp = compareValues(value, rhsVal);
            return switch (op) {
                case ">"  -> cmp > 0;
                case "<"  -> cmp < 0;
                case ">=" -> cmp >= 0;
                case "<=" -> cmp <= 0;
                case "="  -> cmp == 0;
                case "!=" -> cmp != 0;
                default   -> true;
            };
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object a, Object b) {
        if (a instanceof Comparable ca) return ca.compareTo(b);
        return a.toString().compareTo(b.toString());
    }
}
