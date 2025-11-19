package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
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
}