package com.entrodb.catalog;

import java.util.*;
import java.io.*;
import java.nio.file.*;

/**
 * Persists table schemas and constraints to data/catalog.ncat
 *
 * Format:
 *   TABLE:tableName
 *   COL:name:TYPE:nullable:pk
 *   CONSTRAINT:TYPE:column:expression
 *   END
 */
public class CatalogManager {

    private final Map<String, TableSchema> schemas = new HashMap<>();
    private final Path catalogFile;

    public CatalogManager(String dataDir) throws IOException {
        this.catalogFile = Paths.get(dataDir, "catalog.ncat");
        if (Files.exists(catalogFile)) load();
    }

    public void createTable(TableSchema schema) throws IOException {
        String name = schema.getTableName().toLowerCase();
        if (schemas.containsKey(name))
            throw new RuntimeException("Table already exists: " + name);
        schemas.put(name, schema);
        save();
    }

    public TableSchema getTable(String name) {
        TableSchema schema = schemas.get(name.toLowerCase());
        if (schema == null) throw new RuntimeException("Table not found: " + name);
        return schema;
    }

    public boolean tableExists(String name) {
        return schemas.containsKey(name.toLowerCase());
    }

    public void dropTable(String name) throws IOException {
        if (schemas.remove(name.toLowerCase()) == null)
            throw new RuntimeException("Table not found: " + name);
        save();
    }

    public Collection<TableSchema> getAllTables() { return schemas.values(); }

    // ── Persistence ───────────────────────────────────────────────

    private void save() throws IOException {
        List<String> lines = new ArrayList<>();
        for (TableSchema s : schemas.values()) {
            lines.add("TABLE:" + s.getTableName());
            for (Column c : s.getColumns())
                lines.add("COL:" + c.name() + ":" + c.type() + ":"
                    + c.nullable() + ":" + c.primaryKey());
            // Save explicit (non-auto-generated) constraints — skip PK/NN/UNIQUE
            // that come from column defs since those are re-generated on load
            for (Constraint c : s.getConstraints())
                if (c.getType() == Constraint.Type.CHECK)
                    lines.add("CONSTRAINT:" + c.serialize());
            lines.add("END");
        }
        Files.write(catalogFile, lines);
    }

    private void load() throws IOException {
        List<String> lines = Files.readAllLines(catalogFile);
        String tableName = null;
        List<Column>     cols        = new ArrayList<>();
        List<Constraint> constraints = new ArrayList<>();

        for (String line : lines) {
            if (line.startsWith("TABLE:")) {
                tableName   = line.substring(6);
                cols        = new ArrayList<>();
                constraints = new ArrayList<>();
            } else if (line.startsWith("COL:")) {
                String[] p = line.substring(4).split(":");
                cols.add(new Column(p[0], Column.DataType.valueOf(p[1]),
                    Boolean.parseBoolean(p[2]), Boolean.parseBoolean(p[3])));
            } else if (line.startsWith("CONSTRAINT:")) {
                constraints.add(Constraint.deserialize(line.substring(11)));
            } else if (line.equals("END") && tableName != null) {
                TableSchema schema = new TableSchema(tableName, cols, constraints);
                schemas.put(tableName.toLowerCase(), schema);
                tableName = null;
            }
        }
    }
}
