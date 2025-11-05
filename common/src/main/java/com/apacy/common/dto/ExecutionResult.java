package com.apacy.common.dto;

import java.util.List;

/**
 * Data Transfer Object for execution results.
 * 
 * @param success Whether the operation was successful
 * @param message Result message or error description
 * @param data List of rows returned by SELECT operations (null for non-SELECT operations)
 * @param rowsAffected Number of rows affected by the operation
 * @param executionTimeMs Execution time in milliseconds
 * @param transactionId The ID of the transaction that executed the operation
 */
public record ExecutionResult(
    boolean success,
    String message,
    List<Row> data,
    int rowsAffected,
    long executionTimeMs,
    String transactionId
) {
    public ExecutionResult {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (rowsAffected < 0) {
            throw new IllegalArgumentException("Rows affected cannot be negative");
        }
        if (executionTimeMs < 0) {
            throw new IllegalArgumentException("Execution time cannot be negative");
        }
    }
}