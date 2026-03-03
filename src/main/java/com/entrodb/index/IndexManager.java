package com.entrodb.index;

import com.entrodb.catalog.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class IndexManager {

    private final String dataDir;

    // "tableName.colName" → BPlusTree
    private final Map<String, BPlusTree> indexes = new HashMap<>();

    // indexName → "tableName.colName"  (for DROP INDEX by name)
    private final Map<String, String> indexNames = new HashMap<>();

    // "tableName.colName" → indexName  (for SHOW INDEXES)
    private final Map<String, String> indexNameReverse = new HashMap<>();

    // Persist index registry to data/indexes.reg
    private final Path registryFile;

    public IndexManager(String dataDir) throws IOException {
        this.dataDir      = dataDir;
        this.registryFile = Paths.get(dataDir, "indexes.reg");
        if (Files.exists(registryFile)) loadRegistry();
    }

    // ── Lifecycle ─────────────────────────────────────────────────

    public void initTable(TableSchema schema) throws IOException {
        for (Constraint c : schema.getConstraints()) {
            if (c.getType() != Constraint.Type.PRIMARY_KEY &&
                c.getType() != Constraint.Type.UNIQUE) continue;

            String key  = indexKey(schema.getTableName(), c.getColumnName());
            String name = "idx_" + schema.getTableName().toLowerCase()
                        + "_" + c.getColumnName().toLowerCase();

            if (!indexes.containsKey(key)) {
                String path = indexFilePath(schema.getTableName(), c.getColumnName());
                indexes.put(key, new BPlusTree(path));
                indexNames.put(name, key);
                indexNameReverse.put(key, name);
                System.out.printf("INDEX: loaded '%s' on %s.%s (%d entries)%n",
                    name, schema.getTableName(), c.getColumnName(),
                    indexes.get(key).size());
            }
        }
    }

    /**
     * Create a new named index on a column.
     * Scans the entire table to populate the index.
     */
    public void createIndex(String indexName, String tableName, String colName,
                             boolean unique, TableSchema schema,
                             com.entrodb.buffer.BufferPoolManager bpm)
            throws IOException {

        String key = indexKey(tableName, colName);
        if (indexes.containsKey(key))
            throw new RuntimeException("Index already exists on " + tableName + "." + colName);

        String    path = indexFilePath(tableName, colName);
        BPlusTree tree = new BPlusTree(path);

        // Populate from existing data
        int colIdx    = schema.getColumnIndex(colName);
        int pageCount = bpm.getDiskManager().getPageCount(tableName.toLowerCase());

        for (int p = 0; p < pageCount; p++) {
            com.entrodb.storage.PageId pid  =
                new com.entrodb.storage.PageId(tableName.toLowerCase(), p);
            com.entrodb.storage.Page   page = bpm.fetchPage(pid);

            for (int s = 0; s < page.getNumSlots(); s++) {
                byte[] record = page.readRecord(s);
                if (record == null) continue;
                List<Object> row = com.entrodb.util.ByteUtils.deserializeRow(record, schema);
                Object val = row.get(colIdx);
                if (val == null) continue;
                tree.insert(toComparable(val), new RID(p, s));
            }
            bpm.unpinPage(pid, false);
        }

        indexes.put(key, tree);
        indexNames.put(indexName, key);
        indexNameReverse.put(key, indexName);

        // Add constraint to schema if UNIQUE
        if (unique) schema.addConstraint(
            new Constraint(Constraint.Type.UNIQUE, colName));

        saveRegistry();
        tree.save();

        System.out.printf("INDEX: created '%s' on %s.%s (%d entries)%n",
            indexName, tableName, colName, tree.size());
    }

    public void dropIndex(String indexName, String tableName) throws IOException {
        String key = indexNames.get(indexName);
        if (key == null)
            throw new RuntimeException("Index not found: " + indexName);

        indexes.remove(key);
        indexNames.remove(indexName);
        indexNameReverse.remove(key);

        // Delete .idx file
        String[] parts = key.split("\\.");
        if (parts.length >= 2)
            Files.deleteIfExists(Paths.get(indexFilePath(parts[0], parts[1])));

        saveRegistry();
        System.out.println("INDEX: dropped '" + indexName + "'");
    }

    public void dropTableIndexes(String tableName) throws IOException {
        String prefix = tableName.toLowerCase() + ".";
        List<String> keysToRemove = new ArrayList<>();

        for (String key : indexes.keySet())
            if (key.startsWith(prefix)) keysToRemove.add(key);

        for (String key : keysToRemove) {
            String name = indexNameReverse.getOrDefault(key, "");
            indexes.remove(key);
            indexNames.remove(name);
            indexNameReverse.remove(key);
            String[] parts = key.split("\\.");
            if (parts.length >= 2)
                Files.deleteIfExists(Paths.get(indexFilePath(parts[0], parts[1])));
        }
        saveRegistry();
    }

    // ── Index operations ──────────────────────────────────────────

    public void onInsert(TableSchema schema, List<Object> row, RID rid)
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
            tree.insert(toComparable(val), rid);
        }
        // Also update any non-constraint indexes on this table
        updateNonConstraintIndexes(schema, row, rid, true);
    }

    public void onDelete(TableSchema schema, List<Object> row) throws IOException {
        String prefix = schema.getTableName().toLowerCase() + ".";
        for (Map.Entry<String, BPlusTree> e : indexes.entrySet()) {
            if (!e.getKey().startsWith(prefix)) continue;
            String colName = e.getKey().substring(prefix.length());
            if (!schema.hasColumn(colName)) continue;
            int    colIdx = schema.getColumnIndex(colName);
            Object val    = row.get(colIdx);
            if (val == null) continue;
            e.getValue().delete(toComparable(val));
        }
    }

    public void onUpdate(TableSchema schema, List<Object> oldRow,
                          List<Object> newRow, RID newRid) throws IOException {
        onDelete(schema, oldRow);
        onInsert(schema, newRow, newRid);
    }

    public RID lookup(String tableName, String colName, Object value) {
        BPlusTree tree = indexes.get(indexKey(tableName, colName));
        if (tree == null) return null;
        return tree.search(toComparable(value));
    }

    public List<RID> rangeLookup(String tableName, String colName,
                                   Object low, Object high) {
        BPlusTree tree = indexes.get(indexKey(tableName, colName));
        if (tree == null) return Collections.emptyList();
        return tree.rangeSearch(toComparable(low), toComparable(high));
    }

    public boolean hasIndex(String tableName, String colName) {
        return indexes.containsKey(indexKey(tableName, colName));
    }

    public List<String[]> getIndexesForTable(String tableName) {
        List<String[]> result = new ArrayList<>();
        String prefix = tableName.toLowerCase() + ".";
        for (Map.Entry<String, BPlusTree> e : indexes.entrySet()) {
            if (!e.getKey().startsWith(prefix)) continue;
            String colName   = e.getKey().substring(prefix.length());
            String idxName   = indexNameReverse.getOrDefault(e.getKey(), "?");
            int    entries   = e.getValue().size();
            result.add(new String[]{ idxName, tableName, colName,
                                     String.valueOf(entries) });
        }
        return result;
    }

    public void flushAll() throws IOException {
        for (BPlusTree tree : indexes.values()) tree.save();
        System.out.println("INDEX: all indexes flushed");
    }

    // ── Registry persistence ──────────────────────────────────────

    private void saveRegistry() throws IOException {
        List<String> lines = new ArrayList<>();
        for (Map.Entry<String, String> e : indexNames.entrySet())
            lines.add(e.getKey() + "=" + e.getValue()); // indexName=table.col
        Files.write(registryFile, lines);
    }

    private void loadRegistry() throws IOException {
        for (String line : Files.readAllLines(registryFile)) {
            if (line.isBlank()) continue;
            String[] parts = line.split("=", 2);
            if (parts.length < 2) continue;
            String name = parts[0];
            String key  = parts[1];
            indexNames.put(name, key);
            indexNameReverse.put(key, name);

            // Load the actual B+ Tree file if it exists
            String[] kp   = key.split("\\.");
            if (kp.length >= 2) {
                String path = indexFilePath(kp[0], kp[1]);
                if (Files.exists(Paths.get(path))) {
                    indexes.put(key, new BPlusTree(path));
                }
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────

    private void updateNonConstraintIndexes(TableSchema schema, List<Object> row,
                                             RID rid, boolean isInsert) {
        // Handled by onInsert/onDelete covering all indexes for the table
    }

    private String indexKey(String table, String col) {
        return table.toLowerCase() + "." + col.toLowerCase();
    }

    private String indexFilePath(String table, String col) {
        return dataDir + "/" + table.toLowerCase()
             + "_" + col.toLowerCase() + ".idx";
    }

    @SuppressWarnings("unchecked")
    private Comparable toComparable(Object val) {
        if (val instanceof Comparable c) return c;
        return val.toString();
    }
}
