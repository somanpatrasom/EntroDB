package com.nexusdb.index;

import com.nexusdb.catalog.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/**
 * Manages all B+ Tree indexes for all tables.
 *
 * - Auto-creates a PK index when a table is created
 * - Provides insert/delete/search through a single interface
 * - Index files: data/tablename_colname.idx
 */
public class IndexManager {

    private final String dataDir;
    // "tableName.colName" → BPlusTree
    private final Map<String, BPlusTree> indexes = new HashMap<>();

    public IndexManager(String dataDir) {
        this.dataDir = dataDir;
    }

    // ── Lifecycle ─────────────────────────────────────────────────

    /** Load or create all indexes for a table based on its schema. */
    public void initTable(TableSchema schema) throws IOException {
        for (Constraint c : schema.getConstraints()) {
            if (c.getType() == Constraint.Type.PRIMARY_KEY ||
                c.getType() == Constraint.Type.UNIQUE) {
                String key = indexKey(schema.getTableName(), c.getColumnName());
                if (!indexes.containsKey(key)) {
                    String path = indexFilePath(schema.getTableName(), c.getColumnName());
                    indexes.put(key, new BPlusTree(path));
                    System.out.printf("INDEX: loaded '%s' (%d entries)%n",
                        key, indexes.get(key).size());
                }
            }
        }
    }

    /** Drop all indexes for a table (called on DROP TABLE). */
    public void dropTableIndexes(String tableName) throws IOException {
        List<String> toRemove = new ArrayList<>();
        for (String key : indexes.keySet())
            if (key.startsWith(tableName.toLowerCase() + "."))
                toRemove.add(key);

        for (String key : toRemove) {
            indexes.remove(key);
            String[] parts = key.split("\\.");
            Files.deleteIfExists(Paths.get(indexFilePath(parts[0], parts[1])));
        }
    }

    // ── Index operations ──────────────────────────────────────────

    /**
     * Called after every successful INSERT.
     * indexes all indexed columns in the row.
     */
    public void onInsert(TableSchema schema, List<Object> row, RID rid)
            throws IOException {
        for (Constraint c : schema.getConstraints()) {
            if (c.getType() != Constraint.Type.PRIMARY_KEY &&
                c.getType() != Constraint.Type.UNIQUE) continue;

            String key  = indexKey(schema.getTableName(), c.getColumnName());
            BPlusTree tree = indexes.get(key);
            if (tree == null) continue;

            int    colIdx = schema.getColumnIndex(c.getColumnName());
            Object val    = row.get(colIdx);
            if (val == null) continue;

            tree.insert(toComparable(val), rid);
        }
    }

    /**
     * Called before every DELETE.
     * Removes the key from all relevant indexes.
     */
    public void onDelete(TableSchema schema, List<Object> row)
            throws IOException {
        for (Constraint c : schema.getConstraints()) {
            if (c.getType() != Constraint.Type.PRIMARY_KEY &&
                c.getType() != Constraint.Type.UNIQUE) continue;

            String    key  = indexKey(schema.getTableName(), c.getColumnName());
            BPlusTree tree = indexes.get(key);
            if (tree == null) continue;

            int    colIdx = schema.getColumnIndex(c.getColumnName());
            Object val    = row.get(colIdx);
            if (val == null) continue;

            tree.delete(toComparable(val));
        }
    }

    /**
     * Called on UPDATE — remove old key, insert new key.
     */
    public void onUpdate(TableSchema schema, List<Object> oldRow,
                          List<Object> newRow, RID newRid)
            throws IOException {
        onDelete(schema, oldRow);
        onInsert(schema, newRow, newRid);
    }

    /**
     * Look up a single key. Returns null if no index or key not found.
     */
    public RID lookup(String tableName, String colName, Object value) {
        String    key  = indexKey(tableName, colName);
        BPlusTree tree = indexes.get(key);
        if (tree == null) return null;
        return tree.search(toComparable(value));
    }

    /**
     * Range lookup. Returns empty list if no index exists.
     */
    public List<RID> rangeLookup(String tableName, String colName,
                                   Object low, Object high) {
        String    key  = indexKey(tableName, colName);
        BPlusTree tree = indexes.get(key);
        if (tree == null) return Collections.emptyList();
        return tree.rangeSearch(toComparable(low), toComparable(high));
    }

    public boolean hasIndex(String tableName, String colName) {
        return indexes.containsKey(indexKey(tableName, colName));
    }

    /** Flush all index files to disk. Called on checkpoint/shutdown. */
    public void flushAll() throws IOException {
        for (Map.Entry<String, BPlusTree> e : indexes.entrySet())
            e.getValue().save();
        System.out.println("INDEX: all indexes flushed");
    }

    // ── Helpers ───────────────────────────────────────────────────

    private String indexKey(String table, String col) {
        return table.toLowerCase() + "." + col.toLowerCase();
    }

    private String indexFilePath(String table, String col) {
        return dataDir + "/" + table.toLowerCase() + "_" + col.toLowerCase() + ".idx";
    }

    @SuppressWarnings("unchecked")
    private Comparable toComparable(Object val) {
        if (val instanceof Comparable c) return c;
        return val.toString();
    }
}
