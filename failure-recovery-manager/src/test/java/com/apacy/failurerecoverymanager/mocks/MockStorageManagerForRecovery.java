package com.apacy.failurerecoverymanager.mocks;

import com.apacy.common.dto.*;
import com.apacy.storagemanager.StorageManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mock StorageManager for testing LogReplayer.
 * Tracks writeBlock and deleteBlock calls to verify undo/redo operations.
 */
public class MockStorageManagerForRecovery extends StorageManager {

    private final List<DataWrite> writeOperations = new ArrayList<>();
    private final List<DataDeletion> deleteOperations = new ArrayList<>();

    public MockStorageManagerForRecovery() {
        super("test-data-recovery");
    }

    @Override
    public int writeBlock(DataWrite dataWrite) {
        writeOperations.add(dataWrite);
        System.out.println("[MOCK] writeBlock called: " + dataWrite.tableName() + " -> " + dataWrite.newData());
        return 1;
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        deleteOperations.add(dataDeletion);
        System.out.println(
                "[MOCK] deleteBlock called: " + dataDeletion.tableName() + " -> " + dataDeletion.filterCondition());
        return 1;
    }

    // Inspection methods for tests
    public List<DataWrite> getWriteOperations() {
        return new ArrayList<>(writeOperations);
    }

    public List<DataDeletion> getDeleteOperations() {
        return new ArrayList<>(deleteOperations);
    }

    public void clearOperations() {
        writeOperations.clear();
        deleteOperations.clear();
    }

    public int getWriteCount() {
        return writeOperations.size();
    }

    public int getDeleteCount() {
        return deleteOperations.size();
    }

    public DataWrite getLastWrite() {
        return writeOperations.isEmpty() ? null : writeOperations.get(writeOperations.size() - 1);
    }

    public DataDeletion getLastDelete() {
        return deleteOperations.isEmpty() ? null : deleteOperations.get(deleteOperations.size() - 1);
    }
}
