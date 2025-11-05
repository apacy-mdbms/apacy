package com.apacy.common.interfaces;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;

/**
 * Kontrak untuk: Failure Recovery Manager
 * Tugas: Mencatat log dan memulihkan data jika terjadi kegagalan.
 */
public interface IFailureRecoveryManager {
    
    void writeLog(ExecutionResult info);

    void saveCheckpoint();

    void recover(RecoveryCriteria criteria);
}
