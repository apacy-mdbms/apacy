package com.apacy.common.dto;

import java.util.Map;

/**
 * Data Transfer Object for data write operations (INSERT/UPDATE).
 * 
 * @param tableName The name of the table to write to
 * @param data Map of column names to values for INSERT/UPDATE
 * @param whereClause Optional WHERE clause for UPDATE operations
 * @param isUpdate True if this is an UPDATE operation, false for INSERT
 * @param transactionId The ID of the transaction performing the write
 */
public record DataWrite(
    String tableName,
    Map<String, Object> data,
    String whereClause,
    boolean isUpdate,
    String transactionId
) {
    public DataWrite {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }
    }
}