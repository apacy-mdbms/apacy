package com.apacy.common.interfaces;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ExecutionResult;

/**
 * Interface for Concurrency Control Manager.
 * Manages concurrent access to database resources and ensures data consistency.
 */
public interface IConcurrencyControlManager {
    
    /**
     * Begin a new transaction.
     * 
     * @param transactionId Unique identifier for the transaction
     * @return ExecutionResult indicating success or failure
     */
    ExecutionResult beginTransaction(String transactionId);
    
    /**
     * Commit a transaction.
     * 
     * @param transactionId The transaction ID to commit
     * @return ExecutionResult indicating success or failure
     */
    ExecutionResult commitTransaction(String transactionId);
    
    /**
     * Rollback a transaction.
     * 
     * @param transactionId The transaction ID to rollback
     * @return ExecutionResult indicating success or failure
     */
    ExecutionResult rollbackTransaction(String transactionId);
    
    /**
     * Request a read lock for data retrieval operations.
     * 
     * @param retrieval The data retrieval request
     * @return ExecutionResult indicating whether the lock was granted
     */
    ExecutionResult requestReadLock(DataRetrieval retrieval);
    
    /**
     * Request a write lock for data modification operations.
     * 
     * @param write The data write request
     * @return ExecutionResult indicating whether the lock was granted
     */
    ExecutionResult requestWriteLock(DataWrite write);
    
    /**
     * Request a write lock for data deletion operations.
     * 
     * @param deletion The data deletion request
     * @return ExecutionResult indicating whether the lock was granted
     */
    ExecutionResult requestWriteLock(DataDeletion deletion);
    
    /**
     * Release locks held by a transaction.
     * 
     * @param transactionId The transaction ID whose locks should be released
     * @return ExecutionResult indicating success or failure
     */
    ExecutionResult releaseLocks(String transactionId);
    
    /**
     * Check if a transaction can proceed without violating concurrency constraints.
     * 
     * @param transactionId The transaction ID to validate
     * @return ExecutionResult indicating validation result
     */
    ExecutionResult validateTransaction(String transactionId);
    
    /**
     * Detect and resolve deadlocks in the system.
     * 
     * @return ExecutionResult containing deadlock detection results
     */
    ExecutionResult detectDeadlock();
    
    /**
     * Get the current status of a transaction.
     * 
     * @param transactionId The transaction ID to check
     * @return ExecutionResult containing transaction status
     */
    ExecutionResult getTransactionStatus(String transactionId);
}