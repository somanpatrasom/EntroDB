package com.nexusdb.storage;

public record PageId(String tableId, int pageNumber) {
    @Override
    public String toString() {
        return tableId + ":" + pageNumber;
    }
}
