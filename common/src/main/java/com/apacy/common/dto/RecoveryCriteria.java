package com.apacy.common.dto;

import java.time.LocalDateTime;

/**
 * Data Transfer Object for recovery criteria.
 * 
 * @param transactionId The ID of the transaction to recover
 * @param recoveryType Type of recovery (UNDO, REDO, CHECKPOINT)
 * @param logStartPosition Starting position in the log file
 * @param logEndPosition Ending position in the log file
 * @param checkpointTime Time of the checkpoint
 * @param targetTime Target time for point-in-time recovery
 * @param tableName Specific table to recover (null for all tables)
 */
public record RecoveryCriteria(
    String transactionId,
    String recoveryType,
    long logStartPosition,
    long logEndPosition,
    LocalDateTime checkpointTime,
    LocalDateTime targetTime,
    String tableName
) {
    public RecoveryCriteria {
        if (recoveryType == null || recoveryType.trim().isEmpty()) {
            throw new IllegalArgumentException("Recovery type cannot be null or empty");
        }
        if (logStartPosition < 0) {
            throw new IllegalArgumentException("Log start position cannot be negative");
        }
        if (logEndPosition < logStartPosition) {
            throw new IllegalArgumentException("Log end position cannot be less than start position");
        }
    }
}