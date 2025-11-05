package com.apacy.common.interfaces;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;

/**
 * Interface for Failure Recovery Manager.
 * Manages logging, checkpointing, and recovery operations for database consistency.
 */
public interface IFailureRecoveryManager {
    
    /**
     * Write a log entry for a database operation.
     * 
     * @param transactionId The transaction ID performing the operation
     * @param operation The operation being performed (INSERT, UPDATE, DELETE, etc.)
     * @param tableName The table involved in the operation
     * @param oldData The old data (for UPDATE/DELETE operations)
     * @param newData The new data (for INSERT/UPDATE operations)
     * @return ExecutionResult indicating success or failure of logging
     */
    ExecutionResult writeLog(String transactionId, String operation, String tableName, 
                           Object oldData, Object newData);
    
    /**
     * Create a checkpoint to establish a recovery point.
     * 
     * @return ExecutionResult containing checkpoint information
     */
    ExecutionResult saveCheckpoint();
    
    /**
     * Recover the database to a consistent state after a failure.
     * 
     * @param criteria The recovery criteria specifying what to recover
     * @return ExecutionResult indicating recovery success or failure
     */
    ExecutionResult recover(RecoveryCriteria criteria);
    
    /**
     * Perform UNDO operations for uncommitted transactions.
     * 
     * @param transactionId The transaction ID to undo (null for all uncommitted)
     * @return ExecutionResult indicating undo success or failure
     */
    ExecutionResult undo(String transactionId);
    
    /**
     * Perform REDO operations for committed transactions after the last checkpoint.
     * 
     * @param fromCheckpoint Whether to start redo from the last checkpoint
     * @return ExecutionResult indicating redo success or failure
     */
    ExecutionResult redo(boolean fromCheckpoint);
    
    /**
     * Get the current log file status and statistics.
     * 
     * @return ExecutionResult containing log file information
     */
    ExecutionResult getLogStatus();
    
    /**
     * Truncate the log file up to a specific checkpoint.
     * 
     * @param checkpointId The checkpoint ID up to which to truncate
     * @return ExecutionResult indicating truncation success or failure
     */
    ExecutionResult truncateLog(String checkpointId);
    
    /**
     * Backup the current log file.
     * 
     * @param backupPath The path where to store the log backup
     * @return ExecutionResult indicating backup success or failure
     */
    ExecutionResult backupLog(String backupPath);
    
    /**
     * Restore log file from a backup.
     * 
     * @param backupPath The path of the log backup to restore
     * @return ExecutionResult indicating restore success or failure
     */
    ExecutionResult restoreLog(String backupPath);
    
    /**
     * Write a log entry for a database operation.
     * 
     * @param transactionId The transaction ID
     * @param operation The operation type (INSERT, UPDATE, DELETE, etc.)
     * @param tableName The table name involved
     * @param deletion Data deletion information (for DELETE operations)
     * @param data Operation data (for INSERT/UPDATE operations)
     * @return ExecutionResult indicating success or failure
     */
    ExecutionResult writeLog(String transactionId, String operation, String tableName, 
                           com.apacy.common.dto.DataDeletion deletion, java.util.Map<String, Object> data);
}