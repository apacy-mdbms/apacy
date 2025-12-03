package com.apacy.common.interfaces;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;

/**
 * Kontrak untuk: Failure Recovery Manager
 * Tugas: Mencatat log dan memulihkan data jika terjadi kegagalan.
 */
public interface IFailureRecoveryManager {

    void writeLog(ExecutionResult info);

    void writeDataLog(String transactionId, String operation, String tableName, Row dataBefore, Row dataAfter);

    void writeTransactionLog(int transactionId, String lifecycleEvent);

    void saveCheckpoint();

    void recover(RecoveryCriteria criteria);
}
