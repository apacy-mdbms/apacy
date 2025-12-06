package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IStorageManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock implementasi dari IStorageManager untuk keperluan testing Query Processor.
 *
 * Fitur:
 * 1. Stubbing: Mengatur return value untuk method read (readBlock, getSchema, dll).
 * 2. Spying: Melacak jumlah panggilan dan argumen terakhir untuk method write (writeBlock, deleteBlock, dll).
 */
public class MockStorageManager implements IStorageManager {

    // --- Configuration (Stubbing) ---
    private List<Row> rowsToReturn = new ArrayList<>();
    private final Map<String, Schema> schemaMap = new HashMap<>();
    private Map<String, Statistic> allStatsToReturn = new HashMap<>();
    private List<String> dependentTablesToReturn = new ArrayList<>();
    
    private int writeAffectedRowsToReturn = 1;
    private int updateAffectedRowsToReturn = 1;
    private int deleteAffectedRowsToReturn = 1;

    // --- State (Spying) ---
    private int readBlockCallCount = 0;
    private DataRetrieval lastDataRetrieval;

    private int writeBlockCallCount = 0;
    private DataWrite lastDataWrite;

    private int updateBlockCallCount = 0;
    private DataUpdate lastDataUpdate;

    private int deleteBlockCallCount = 0;
    private DataDeletion lastDataDeletion;

    private int createTableCallCount = 0;
    private Schema lastCreatedSchema;

    private int dropTableCallCount = 0;
    private String lastDroppedTable;

    private int setIndexCallCount = 0;
    private int dropIndexCallCount = 0;

    // --- Configuration Methods ---

    public void setRowsToReturn(List<Row> rows) {
        this.rowsToReturn = rows;
    }

    public void addSchema(String tableName, Schema schema) {
        this.schemaMap.put(tableName, schema);
    }

    public void setAllStatsToReturn(Map<String, Statistic> stats) {
        this.allStatsToReturn = stats;
    }

    public void setDependentTablesToReturn(List<String> tables) {
        this.dependentTablesToReturn = tables;
    }
    
    public void setWriteAffectedRowsToReturn(int rows) {
        this.writeAffectedRowsToReturn = rows;
    }

    // --- MISSING SETTERS FIXED HERE ---
    public void setUpdateAffectedRowsToReturn(int rows) {
        this.updateAffectedRowsToReturn = rows;
    }

    public void setDeleteAffectedRowsToReturn(int rows) {
        this.deleteAffectedRowsToReturn = rows;
    }
    // ----------------------------------

    // --- Interface Implementation ---

    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        this.readBlockCallCount++;
        this.lastDataRetrieval = dataRetrieval;
        // Return defensive copy agar test aman
        return new ArrayList<>(this.rowsToReturn);
    }

    @Override
    public int writeBlock(DataWrite dataWrite) {
        this.writeBlockCallCount++;
        this.lastDataWrite = dataWrite;
        return this.writeAffectedRowsToReturn;
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        this.deleteBlockCallCount++;
        this.lastDataDeletion = dataDeletion;
        return this.deleteAffectedRowsToReturn;
    }

    @Override
    public int updateBlock(DataUpdate dataUpdate) {
        this.updateBlockCallCount++;
        this.lastDataUpdate = dataUpdate;
        return this.updateAffectedRowsToReturn;
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        this.setIndexCallCount++;
    }

    @Override
    public void dropIndex(String tableName, String indexName) {
        this.dropIndexCallCount++;
    }

    @Override
    public Map<String, Statistic> getAllStats() {
        return this.allStatsToReturn;
    }

    @Override
    public void createTable(Schema schema) throws IOException {
        this.createTableCallCount++;
        this.lastCreatedSchema = schema;
        // Secara otomatis simpan schema ini agar bisa di-retrieve oleh getSchema nanti
        if (schema != null) {
            this.schemaMap.put(schema.tableName(), schema);
        }
    }

    @Override
    public Schema getSchema(String tableName) {
        return this.schemaMap.get(tableName);
    }

    @Override
    public int dropTable(String tableName, String option) {
        this.dropTableCallCount++;
        this.lastDroppedTable = tableName;
        this.schemaMap.remove(tableName);
        return 1; // Default 1 table dropped
    }

    @Override
    public List<String> getDependentTables(String tablename) {
        return this.dependentTablesToReturn;
    }

    // --- Helper Methods untuk Verifikasi Test ---

    public int getReadBlockCallCount() { return readBlockCallCount; }
    public DataRetrieval getLastDataRetrieval() { return lastDataRetrieval; }

    public int getWriteBlockCallCount() { return writeBlockCallCount; }
    public DataWrite getLastDataWrite() { return lastDataWrite; }

    public int getUpdateBlockCallCount() { return updateBlockCallCount; }
    public DataUpdate getLastDataUpdate() { return lastDataUpdate; }

    public int getDeleteBlockCallCount() { return deleteBlockCallCount; }
    public DataDeletion getLastDataDeletion() { return lastDataDeletion; }

    public int getCreateTableCallCount() { return createTableCallCount; }
    public Schema getLastCreatedSchema() { return lastCreatedSchema; }
    
    public int getDropTableCallCount() { return dropTableCallCount; }
    public String getLastDroppedTable() { return lastDroppedTable; }

    public void reset() {
        this.readBlockCallCount = 0;
        this.writeBlockCallCount = 0;
        this.updateBlockCallCount = 0;
        this.deleteBlockCallCount = 0;
        this.createTableCallCount = 0;
        this.dropTableCallCount = 0;
        this.setIndexCallCount = 0;
        this.dropIndexCallCount = 0;
        
        this.lastDataRetrieval = null;
        this.lastDataWrite = null;
        this.lastDataUpdate = null;
        this.lastDataDeletion = null;
        this.lastCreatedSchema = null;
        this.lastDroppedTable = null;
        
        this.rowsToReturn.clear();
        this.schemaMap.clear();
        this.allStatsToReturn.clear();
        this.dependentTablesToReturn.clear();
        
        // Reset config values to defaults
        this.writeAffectedRowsToReturn = 1;
        this.updateAffectedRowsToReturn = 1;
        this.deleteAffectedRowsToReturn = 1;
    }
}