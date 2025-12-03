package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.common.interfaces.IFailureRecoveryManager;

public class MockFailureRecoveryManager implements IFailureRecoveryManager {

    @Override
    public void writeLog(ExecutionResult info) {
        System.out.println("[MOCK-FRM] writeLog: Tx " + info.transactionId() + " | Op: " + info.operation() + " | Status: " + info.success());
    }

    @Override
    public void saveCheckpoint() {
        System.out.println("[MOCK-FRM] saveCheckpoint dipanggil.");
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        System.out.println("[MOCK-FRM] RECOVER dipanggil. Type: " + criteria.recoveryType() + ", TxId: " + criteria.transactionId());
    }

    @Override
    public void writeDataLog(String transactionId, String operation, String tableName, Row dataBefore, Row dataAfter) {
        return;
    }

    @Override
    public void writeTransactionLog(int transactionId, String lifecycleEvent) {
        return;
    }
}