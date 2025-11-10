package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IFailureRecoveryManager;

public class MockFailureRecoveryManager implements IFailureRecoveryManager {

    @Override
    public void writeLog(ExecutionResult info) {
        System.out.println("[MOCK-FRM] writeLog dipanggil untuk:");
        System.out.println("  - txId: " + info.transactionId());
        System.out.println("  - operation: " + info.operation());
        System.out.println("  - success: " + info.success());
        System.out.println("  - affectedRows: " + info.affectedRows());
        System.out.println("  - message: " + info.message());
    }

    @Override
    public void saveCheckpoint() {
        System.out.println("[MOCK-FRM] saveCheckpoint dipanggil - creating mock checkpoint.");
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        System.out.println("[MOCK-FRM] recover dipanggil:");
        System.out.println("  - recoveryType: " + criteria.recoveryType());
        System.out.println("  - transactionId: " + criteria.transactionId());
        System.out.println("  - targetTime: " + criteria.targetTime());
    }
}
