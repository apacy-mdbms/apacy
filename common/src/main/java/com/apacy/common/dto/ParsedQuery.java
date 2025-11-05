package com.apacy.common.dto;

import java.util.List;

/**
 * Data Transfer Object for parsed SQL queries.
 * 
 * @param queryType The type of SQL operation (SELECT, INSERT, UPDATE, DELETE)
 * @param tableName Primary table name involved in the query
 * @param selectColumns List of columns to select (for SELECT queries)
 * @param whereClause Parsed WHERE clause conditions
 * @param joinClauses List of JOIN operations
 * @param orderByColumns List of columns for ordering
 * @param groupByColumns List of columns for grouping
 * @param havingClause HAVING clause conditions
 * @param insertValues Values for INSERT operations
 * @param updateValues Values for UPDATE operations
 * @param estimatedCost Estimated execution cost
 * @param optimizationHints Hints for query optimization
 */
public record ParsedQuery(
    String queryType,
    String tableName,
    List<String> selectColumns,
    String whereClause,
    List<String> joinClauses,
    List<String> orderByColumns,
    List<String> groupByColumns,
    String havingClause,
    List<Object> insertValues,
    List<Object> updateValues,
    double estimatedCost,
    List<String> optimizationHints
) {
    public ParsedQuery {
        if (queryType == null || queryType.trim().isEmpty()) {
            throw new IllegalArgumentException("Query type cannot be null or empty");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
    }
}