package com.apacy.common.interfaces;

import com.apacy.common.dto.*;

/**
 * Interface for Storage Manager.
 * Manages physical data storage, indexing, and retrieval operations.
 */
public interface IStorageManager {
    
    /**
     * Retrieve data from the database based on specified criteria.
     * 
     * @param retrieval The data retrieval specification
     * @return ExecutionResult containing the retrieved data or error information
     */
    ExecutionResult getData(DataRetrieval retrieval);
    
    /**
     * Write data to the database (INSERT or UPDATE).
     * 
     * @param write The data write specification
     * @return ExecutionResult indicating success or failure of the write operation
     */
    ExecutionResult writeData(DataWrite write);
    
    /**
     * Delete data from the database.
     * 
     * @param deletion The data deletion specification
     * @return ExecutionResult indicating success or failure of the deletion
     */
    ExecutionResult deleteData(DataDeletion deletion);
    
    /**
     * Get statistics about a table for query optimization.
     * 
     * @param tableName The name of the table to get statistics for
     * @return ExecutionResult containing table statistics
     */
    ExecutionResult getStats(String tableName);
    
    /**
     * Create a new table with the specified schema.
     * 
     * @param tableName The name of the new table
     * @param schema The table schema definition
     * @return ExecutionResult indicating success or failure of table creation
     */
    ExecutionResult createTable(String tableName, String schema);
    
    /**
     * Drop an existing table from the database.
     * 
     * @param tableName The name of the table to drop
     * @return ExecutionResult indicating success or failure of table deletion
     */
    ExecutionResult dropTable(String tableName);
    
    /**
     * Create an index on a table column.
     * 
     * @param tableName The name of the table
     * @param columnName The name of the column to index
     * @param indexType The type of index (HASH, BTREE, etc.)
     * @return ExecutionResult indicating success or failure of index creation
     */
    ExecutionResult createIndex(String tableName, String columnName, String indexType);
    
    /**
     * Drop an existing index.
     * 
     * @param tableName The name of the table
     * @param columnName The name of the indexed column
     * @return ExecutionResult indicating success or failure of index deletion
     */
    ExecutionResult dropIndex(String tableName, String columnName);
    
    /**
     * Flush all pending writes to disk.
     * 
     * @return ExecutionResult indicating success or failure of the flush operation
     */
    ExecutionResult flush();
    
    /**
     * Compact the storage to reclaim space from deleted records.
     * 
     * @param tableName The name of the table to compact (null for all tables)
     * @return ExecutionResult indicating success or failure of compaction
     */
    ExecutionResult compact(String tableName);
    
    /**
     * Check the integrity of stored data.
     * 
     * @param tableName The name of the table to check (null for all tables)
     * @return ExecutionResult containing integrity check results
     */
    ExecutionResult checkIntegrity(String tableName);
}