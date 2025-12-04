package com.apacy.concurrencycontrolmanager.mocks;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.common.interfaces.IFailureRecoveryManager;

import java.util.ArrayList;
import java.util.List;

public class MockFailureRecoveryManager implements IFailureRecoveryManager {

    private final List<TransactionLogEntry> transactionLogs = new ArrayList<>();
    private int writeLogCallCount = 0;
    private int saveCheckpointCallCount = 0;
    private int recoverCallCount = 0;
    private int writeDataLogCallCount = 0;

    @Override
    public void writeLog(ExecutionResult info) {
        writeLogCallCount++;
    }

    @Override
    public void saveCheckpoint() {
        saveCheckpointCallCount++;
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        recoverCallCount++;
    }

    @Override
    public void writeDataLog(String transactionId, String operation, String tableName,
            Row dataBefore, Row dataAfter) {
        writeDataLogCallCount++;
    }

    @Override
    public void writeTransactionLog(int transactionId, String lifecycleEvent) {
        transactionLogs.add(new TransactionLogEntry(transactionId, lifecycleEvent));
    }

    // Helper methods for testing
    public List<TransactionLogEntry> getTransactionLogs() {
        return new ArrayList<>(transactionLogs);
    }

    public int getWriteLogCallCount() {
        return writeLogCallCount;
    }

    public int getSaveCheckpointCallCount() {
        return saveCheckpointCallCount;
    }

    public int getRecoverCallCount() {
        return recoverCallCount;
    }

    public int getWriteDataLogCallCount() {
        return writeDataLogCallCount;
    }

    public void reset() {
        transactionLogs.clear();
        writeLogCallCount = 0;
        saveCheckpointCallCount = 0;
        recoverCallCount = 0;
        writeDataLogCallCount = 0;
    }

    /**
     * Helper class to store transaction log entries for verification.
     */
    public static class TransactionLogEntry {
        private final int transactionId;
        private final String lifecycleEvent;

        public TransactionLogEntry(int transactionId, String lifecycleEvent) {
            this.transactionId = transactionId;
            this.lifecycleEvent = lifecycleEvent;
        }

        public int getTransactionId() {
            return transactionId;
        }

        public String getLifecycleEvent() {
            return lifecycleEvent;
        }
    }
}