package com.apacy.common.dto;

/**
 * Data Transfer Object for data deletion operations.
 * 
 * @param tableName The name of the table to delete from
 * @param whereClause Optional WHERE clause for conditional deletion
 * @param transactionId The ID of the transaction performing the deletion
 */
public record DataDeletion(
    String tableName,
    String whereClause,
    String transactionId
) {
    public DataDeletion {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
    }
}