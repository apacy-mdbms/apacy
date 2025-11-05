package com.apacy.common.dto;

import java.util.Map;

/**
 * Data Transfer Object representing a single database row.
 * 
 * @param tableName The name of the table this row belongs to
 * @param data Map of column names to their values
 * @param rowId Unique identifier for this row (can be null for new rows)
 * @param version Version number for optimistic concurrency control
 */
public record Row(
    String tableName,
    Map<String, Object> data,
    String rowId,
    long version
) {
    public Row {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        if (version < 0) {
            throw new IllegalArgumentException("Version cannot be negative");
        }
    }
    
    /**
     * Get the value of a specific column.
     * 
     * @param columnName The name of the column
     * @return The value of the column, or null if not found
     */
    public Object getValue(String columnName) {
        return data.get(columnName);
    }
    
    /**
     * Get the value of a specific column with type casting.
     * 
     * @param <T> The expected type
     * @param columnName The name of the column
     * @param type The class of the expected type
     * @return The value cast to the specified type, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(String columnName, Class<T> type) {
        Object value = data.get(columnName);
        if (value == null || !type.isInstance(value)) {
            return null;
        }
        return (T) value;
    }
}