package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.common.interfaces.IFailureRecoveryManager;

/**
 * Mock implementasi dari IFailureRecoveryManager untuk keperluan testing Query Processor.
 * * Fitur:
 * 1. Spying: Melacak panggilan logging (writeLog, writeDataLog, writeTransactionLog) 
 * untuk memastikan Query Processor mencatat audit trail dengan benar.
 * 2. Verification: Menyediakan getter untuk memeriksa argumen terakhir yang diterima.
 */
public class MockFailureRecoveryManager implements IFailureRecoveryManager {

    // --- State (Spying) ---
    private int writeLogCallCount = 0;
    private ExecutionResult lastExecutionResult;

    private int writeDataLogCallCount = 0;
    private String lastDataLogTransactionId;
    private String lastDataLogOperation;
    private String lastDataLogTableName;
    private Row lastDataLogDataBefore;
    private Row lastDataLogDataAfter;

    private int writeTransactionLogCallCount = 0;
    private int lastTransactionLogTransactionId;
    private String lastTransactionLogLifecycleEvent;

    private int saveCheckpointCallCount = 0;

    private int recoverCallCount = 0;
    private RecoveryCriteria lastRecoveryCriteria;

    // --- Interface Implementation ---

    @Override
    public void writeLog(ExecutionResult info) {
        this.writeLogCallCount++;
        this.lastExecutionResult = info;
    }

    @Override
    public void writeDataLog(String transactionId, String operation, String tableName, Row dataBefore, Row dataAfter) {
        this.writeDataLogCallCount++;
        this.lastDataLogTransactionId = transactionId;
        this.lastDataLogOperation = operation;
        this.lastDataLogTableName = tableName;
        this.lastDataLogDataBefore = dataBefore;
        this.lastDataLogDataAfter = dataAfter;
    }

    @Override
    public void writeTransactionLog(int transactionId, String lifecycleEvent) {
        this.writeTransactionLogCallCount++;
        this.lastTransactionLogTransactionId = transactionId;
        this.lastTransactionLogLifecycleEvent = lifecycleEvent;
    }

    @Override
    public void saveCheckpoint() {
        this.saveCheckpointCallCount++;
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        this.recoverCallCount++;
        this.lastRecoveryCriteria = criteria;
    }

    // --- Helper Methods untuk Verifikasi Test ---

    public int getWriteLogCallCount() {
        return writeLogCallCount;
    }

    public ExecutionResult getLastExecutionResult() {
        return lastExecutionResult;
    }

    public int getWriteDataLogCallCount() {
        return writeDataLogCallCount;
    }

    public String getLastDataLogTransactionId() {
        return lastDataLogTransactionId;
    }

    public String getLastDataLogOperation() {
        return lastDataLogOperation;
    }

    public String getLastDataLogTableName() {
        return lastDataLogTableName;
    }

    public Row getLastDataLogDataBefore() {
        return lastDataLogDataBefore;
    }

    public Row getLastDataLogDataAfter() {
        return lastDataLogDataAfter;
    }

    public int getWriteTransactionLogCallCount() {
        return writeTransactionLogCallCount;
    }

    public int getLastTransactionLogTransactionId() {
        return lastTransactionLogTransactionId;
    }

    public String getLastTransactionLogLifecycleEvent() {
        return lastTransactionLogLifecycleEvent;
    }

    public int getSaveCheckpointCallCount() {
        return saveCheckpointCallCount;
    }

    public int getRecoverCallCount() {
        return recoverCallCount;
    }

    public RecoveryCriteria getLastRecoveryCriteria() {
        return lastRecoveryCriteria;
    }

    public void reset() {
        this.writeLogCallCount = 0;
        this.writeDataLogCallCount = 0;
        this.writeTransactionLogCallCount = 0;
        this.saveCheckpointCallCount = 0;
        this.recoverCallCount = 0;

        this.lastExecutionResult = null;
        this.lastDataLogTransactionId = null;
        this.lastDataLogOperation = null;
        this.lastDataLogTableName = null;
        this.lastDataLogDataBefore = null;
        this.lastDataLogDataAfter = null;
        this.lastTransactionLogLifecycleEvent = null;
        this.lastRecoveryCriteria = null;
        this.lastTransactionLogTransactionId = 0;
    }
}