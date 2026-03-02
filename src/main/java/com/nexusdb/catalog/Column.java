package com.nexusdb.catalog;

public record Column(String name, DataType type, boolean nullable, boolean primaryKey) {

    public enum DataType {
        INT,        // 4 bytes
        BIGINT,     // 8 bytes
        FLOAT,      // 8 bytes (double)
        VARCHAR,    // variable length, max 255
        BOOLEAN     // 1 byte
    }

    public int fixedSize() {
        return switch (type) {
            case INT -> 4;
            case BIGINT -> 8;
            case FLOAT -> 8;
            case BOOLEAN -> 1;
            case VARCHAR -> -1; // variable
        };
    }
}
