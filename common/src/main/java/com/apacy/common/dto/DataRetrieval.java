package com.apacy.common.dto;

import java.util.List;

/**
 * Data Transfer Object for data retrieval operations.
 * 
 * @param tableName The name of the table to retrieve from
 * @param selectColumns List of column names to select (null means SELECT *)
 * @param whereClause Optional WHERE clause for filtering
 * @param orderByClause Optional ORDER BY clause for sorting
 * @param joinClause Optional JOIN clause for joining tables
 * @param groupByClause Optional GROUP BY clause for aggregation
 * @param havingClause Optional HAVING clause for group filtering
 * @param transactionId The ID of the transaction performing the retrieval
 */
public record DataRetrieval(
    String tableName,
    List<String> selectColumns,
    String whereClause,
    String orderByClause,
    String joinClause,
    String groupByClause,
    String havingClause,
    String transactionId
) {
    public DataRetrieval {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
    }
}