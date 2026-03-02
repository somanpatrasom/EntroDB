package com.nexusdb.sql.ast;

import com.nexusdb.catalog.*;
import java.util.List;

public class CreateTableStatement extends Statement {
    public final String tableName;
    public final List<Column> columns;

    public CreateTableStatement(String tableName, List<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override public Type getType() { return Type.CREATE_TABLE; }
}
