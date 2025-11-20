package com.apacy.failurerecoverymanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.storagemanager.StorageManager;

public class FailureRecoveryManager extends DBMSComponent implements IFailureRecoveryManager {

    // Gunakan helper class internal yang sudah dibuat
    private final LogWriter logWriter;
    private final LogReplayer logReplayer;
    private final CheckpointManager checkpointManager;
    protected final StorageManager storageManager;

    public FailureRecoveryManager() {
        super("Failure Recovery Manager");
        this.storageManager = null;
        this.logWriter = new LogWriter();
        this.logReplayer = new LogReplayer();
        this.checkpointManager = new CheckpointManager();
    }

    public FailureRecoveryManager(StorageManager storageManager) {
        super("Failure Recovery Manager");
        this.storageManager = storageManager;
        // Inisialisasi helper-nya
        this.logWriter = new LogWriter();
        this.logReplayer = new LogReplayer();
        this.checkpointManager = new CheckpointManager();
    }

    @Override
    public void initialize() throws Exception {
        System.out.println(this.getComponentName() + " is initializing...");
        // Logika untuk melakukan recovery saat startup jika shutdown sebelumnya tidak
        // normal
        System.out.println(this.getComponentName() + " initialized successfully.");
    }

    @Override
    public void shutdown() {
        System.out.println(this.getComponentName() + " is shutting down...");
        // Selalu buat checkpoint terakhir untuk memastikan data konsisten.
        this.saveCheckpoint();
        System.out.println(this.getComponentName() + " shut down gracefully.");
    }

    @Override
    public void writeLog(ExecutionResult info) {
        if (info == null) {
            System.err.println("ExecutionResult info is null. Cannot write log.");
            return;
        }
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
            System.out.println("Creating a new checkpoint...");
            this.checkpointManager.createCheckpoint();
        } catch (Exception e) {
            System.err.println("Failed to create checkpoint.");
            e.printStackTrace();
        }
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        if (criteria == null) {
            System.err.println("Recovery criteria is null. Aborting recovery process.");
            return;
        }
        try {
            switch (criteria.recoveryType()) {
                case "UNDO_TRANSACTION":
                    this.logReplayer.undoTransaction(criteria.transactionId());
                    break;
                case "POINT_IN_TIME":
                    this.logReplayer.replayLogs(criteria);
                    break;
                default:
                    this.logReplayer.replayLogs(criteria);
                    break;
            }
        } catch (Exception e) {
            System.err.println("A critical error occurred during the recovery process.");
            e.printStackTrace();
        }
    }
}