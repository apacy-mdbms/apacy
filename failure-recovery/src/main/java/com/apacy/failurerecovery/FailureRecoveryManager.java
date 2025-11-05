package com.apacy.failurerecovery;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IFailureRecoveryManager;

public class FailureRecoveryManager extends DBMSComponent implements IFailureRecoveryManager {

    // Gunakan helper class internal yang sudah dibuat
    private final LogWriter logWriter;
    private final LogReplayer logReplayer;
    private final CheckpointManager checkpointManager;

    public FailureRecoveryManager() {
        super("Failure Recovery Manager");
        
        // Inisialisasi helper-nya
        this.logWriter = new LogWriter();
        this.logReplayer = new LogReplayer();
        this.checkpointManager = new CheckpointManager();
    }

    @Override
    public void initialize() throws Exception {
        // ... (Logika inisialisasi jika ada) ...
    }

    @Override
    public void shutdown() {
        // ... (Logika shutdown, misal: logWriter.close()) ...
    }

    @Override
    public void writeLog(ExecutionResult info) {
        // Delegasikan tugas ke helper
        try {
            // Ubah DTO ExecutionResult menjadi format log entry internal Anda
            // (Ini hanya contoh, sesuaikan dengan LogWriter Anda)
            logWriter.writeLog(
                String.valueOf(info.transactionId()),
                info.operation(),
                null, // Anda perlu cara untuk mendapatkan nama tabel
                info.rows() // atau data lain dari 'info'
            );
        } catch (Exception e) {
            // TODO: Handle exception (misal: print error)
            e.printStackTrace();
        }
    }

    @Override
    public void saveCheckpoint() {
        // Delegasikan tugas ke helper
        try {
            this.checkpointManager.createCheckpoint();
        } catch (Exception e) {
            // TODO: Handle exception
            e.printStackTrace();
        }
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        // Delegasikan tugas ke helper
        try {
            if ("UNDO_TRANSACTION".equals(criteria.recoveryType())) {
                this.logReplayer.undoTransaction(criteria.transactionId());
            } else if ("POINT_IN_TIME".equals(criteria.recoveryType())) {
                this.logReplayer.replayLogs(criteria);
            } else {
                this.logReplayer.replayLogs(criteria);
            }
        } catch (Exception e) {
            // TODO: Handle exception
            e.printStackTrace();
        }
    }
}