package com.apacy.storagemanager;

import java.util.List;

public class Schema {
    private List<Column> columns;
    private int fixedRowSize = -1; // Cache

    public Schema(List<Column> columns) {
        this.columns = columns;
    }
    public List<Column> getColumns() { return columns; }
    public Column getColumn(int index) { return columns.get(index); }
    public int getColumnCount() { return columns.size(); }

    public int getFixedRowSize() {
        if (this.fixedRowSize == -1) {
            int totalSize = 0;
            for (Column col : this.columns) {
                totalSize += col.getFixedSize();
            }
            this.fixedRowSize = totalSize;
        }
        return this.fixedRowSize;
    }
}