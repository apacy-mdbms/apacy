package com.apacy.common.enums;

/**
 * Enumeration of possible database actions/operations.
 */
public enum Action {
    /**
     * SELECT operation - reading data from tables.
     */
    SELECT,
    
    /**
     * INSERT operation - adding new data to tables.
     */
    INSERT,
    
    /**
     * UPDATE operation - modifying existing data in tables.
     */
    UPDATE,
    
    /**
     * DELETE operation - removing data from tables.
     */
    DELETE,
    
    /**
     * CREATE_TABLE operation - creating new tables.
     */
    CREATE_TABLE,
    
    /**
     * DROP_TABLE operation - removing tables.
     */
    DROP_TABLE,
    
    /**
     * CREATE_INDEX operation - creating indexes.
     */
    CREATE_INDEX,
    
    /**
     * DROP_INDEX operation - removing indexes.
     */
    DROP_INDEX,
    
    /**
     * BEGIN_TRANSACTION operation - starting a transaction.
     */
    BEGIN_TRANSACTION,
    
    /**
     * COMMIT_TRANSACTION operation - committing a transaction.
     */
    COMMIT_TRANSACTION,
    
    /**
     * ROLLBACK_TRANSACTION operation - rolling back a transaction.
     */
    ROLLBACK_TRANSACTION,
    
    /**
     * CHECKPOINT operation - creating a checkpoint.
     */
    CHECKPOINT,
    
    /**
     * BACKUP operation - creating a backup.
     */
    BACKUP,
    
    /**
     * RESTORE operation - restoring from backup.
     */
    RESTORE;
    
    /**
     * Check if this action is a data modification operation.
     * 
     * @return true if this action modifies data (INSERT, UPDATE, DELETE)
     */
    public boolean isDataModification() {
        return this == INSERT || this == UPDATE || this == DELETE;
    }
    
    /**
     * Check if this action is a schema modification operation.
     * 
     * @return true if this action modifies schema (CREATE_TABLE, DROP_TABLE, etc.)
     */
    public boolean isSchemaModification() {
        return this == CREATE_TABLE || this == DROP_TABLE || 
               this == CREATE_INDEX || this == DROP_INDEX;
    }
    
    /**
     * Check if this action is a transaction control operation.
     * 
     * @return true if this action controls transactions
     */
    public boolean isTransactionControl() {
        return this == BEGIN_TRANSACTION || this == COMMIT_TRANSACTION || 
               this == ROLLBACK_TRANSACTION;
    }
    
    /**
     * Check if this action requires a transaction context.
     * 
     * @return true if this action should be executed within a transaction
     */
    public boolean requiresTransaction() {
        return isDataModification() || isSchemaModification();
    }
}