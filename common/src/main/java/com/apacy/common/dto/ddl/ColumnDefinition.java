package com.apacy.common.dto.ddl;

import com.apacy.common.enums.DataType;

public class ColumnDefinition {
    private final String name;
    private final DataType type;
    private final int length;
    private final boolean isPrimaryKey;

    public ColumnDefinition(String name, DataType type, int length, boolean isPrimaryKey) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.isPrimaryKey = isPrimaryKey;
    }

    // Getters
    public String getName() { return name; }
    public DataType getType() { return type; }
    public int getLength() { return length; }
    public boolean isPrimaryKey() { return isPrimaryKey; }
}