package com.apacy.common.dto;

import java.util.Map;

/**
 * Data Transfer Object for database statistics.
 * 
 * @param tableName Name of the table these statistics belong to
 * @param rowCount Total number of rows in the table
 * @param indexCount Number of indexes on the table
 * @param avgRowSize Average size of a row in bytes
 * @param totalSize Total size of the table in bytes
 * @param lastUpdated Timestamp when statistics were last updated
 * @param columnStats Per-column statistics (cardinality, null count, etc.)
 * @param accessPatterns Information about query access patterns
 */
public record Statistic(
    String tableName,
    long rowCount,
    int indexCount,
    double avgRowSize,
    long totalSize,
    long lastUpdated,
    Map<String, Object> columnStats,
    Map<String, Object> accessPatterns
) {
    public Statistic {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("Row count cannot be negative");
        }
        if (indexCount < 0) {
            throw new IllegalArgumentException("Index count cannot be negative");
        }
        if (avgRowSize < 0) {
            throw new IllegalArgumentException("Average row size cannot be negative");
        }
        if (totalSize < 0) {
            throw new IllegalArgumentException("Total size cannot be negative");
        }
    }
    
    /**
     * Get the selectivity estimate for a column.
     * 
     * @param columnName The name of the column
     * @return Selectivity estimate between 0.0 and 1.0
     */
    public double getSelectivity(String columnName) {
        if (columnStats == null) {
            return 1.0; // Default selectivity
        }
        
        Object cardinality = columnStats.get(columnName + "_cardinality");
        if (cardinality instanceof Number cardNum && rowCount > 0) {
            return Math.min(1.0, cardNum.doubleValue() / rowCount);
        }
        
        return 1.0; // Default selectivity
    }
}