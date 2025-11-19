package com.apacy.failurerecoverymanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IFailureRecoveryManager;

import com.apacy.storagemanager.StorageManager; 

import java.io.IOException;

public class FailureRecoveryManager extends DBMSComponent implements IFailureRecoveryManager {

    private final LogWriter logWriter;
    private final LogReplayer logReplayer;
    private final CheckpointManager checkpointManager;
    protected final StorageManager storageManager;

    protected final String LOG_FILE_PATH = "failure-recovery/log/mDBMS.log";
    protected final String CHECKPOINT_DIR = "failure-recovery/checkpoints";

    public FailureRecoveryManager() {
        super("Failure Recovery Manager");
        this.storageManager = null;
        this.logWriter = new LogWriter();
        this.logReplayer = new LogReplayer();
        this.checkpointManager = new CheckpointManager();
    }

    public FailureRecoveryManager(StorageManager storageManager) {
        super("Failure Recovery Manager");
        System.out.println("Initializing FailureRecoveryManager...");

        this.storageManager = storageManager;

        this.logWriter = new LogWriter(LOG_FILE_PATH);
        this.logReplayer = new LogReplayer(LOG_FILE_PATH);
        this.checkpointManager = new CheckpointManager(CHECKPOINT_DIR);
        
        System.out.println("FailureRecoveryManager initialized successfully.");
    }

    @Override
    public void initialize() throws Exception {
        System.out.println("FailureRecoveryManager component is active.");
    }

    @Override
    public void shutdown() {
        // Logika shutdown, terutama memastikan semua log ter-flush ke disk.
        try {
            System.out.println("Shutting down FailureRecoveryManager...");
            logWriter.flush();
            logWriter.close();
            System.out.println("Log writer flushed and closed. Shutdown complete.");
        } catch (IOException e) {
            System.err.println("Error during FailureRecoveryManager shutdown:");
            e.printStackTrace();
        }
    }

    @Override
    public void writeLog(ExecutionResult info) {
        if (info == null) {
            System.err.println("writeLog dipanggil dengan ExecutionResult null.");
            return;
        }

        try {
            String tableName = null; // TODO: Dapatkan tableName dari DTO ExecutionResult
            Object logData = info.rows(); 

            logWriter.writeLog(
                String.valueOf(info.transactionId()),
                info.operation(),
                tableName,
                logData
            );
        } catch (Exception e) {
            System.err.println("Gagal menulis log untuk Transaksi " + info.transactionId() + ":");
            e.printStackTrace();
            // TODO: Handle exception (misal: paksa transaction abort?)
        }
    }

    @Override
    public void saveCheckpoint() {
        try {
            System.out.println("Memulai proses save checkpoint...");
            this.checkpointManager.createCheckpoint();
            System.out.println("Proses save checkpoint berhasil.");
        } catch (Exception e) {
            System.err.println("Gagal saat save checkpoint:");
            e.printStackTrace();
            // TODO: Handle exception
        }
    }

    @Override
    public void recover(RecoveryCriteria criteria) {
        if (criteria == null || criteria.recoveryType() == null) {
            System.err.println("Proses recovery gagal: RecoveryCriteria null atau tidak valid.");
            return;
        }

        try {
            System.out.println("Memulai proses recovery. Tipe: " + criteria.recoveryType());
            if ("UNDO_TRANSACTION".equals(criteria.recoveryType())) {
                if (criteria.transactionId() == null) {
                    System.err.println("UNDO_TRANSACTION memerlukan transactionId.");
                    return;
                }
                System.out.println("Melakukan UNDO untuk Transaksi: " + criteria.transactionId());
                this.logReplayer.undoTransaction(criteria.transactionId());
            
            } else if ("POINT_IN_TIME".equals(criteria.recoveryType()) || "SYSTEM_RESTART".equals(criteria.recoveryType())) {
                System.out.println("Melakukan replay log berdasarkan kriteria...");
                this.logReplayer.replayLogs(criteria);
            
            } else {
                System.out.println("Tipe recovery tidak dikenal, melakukan replay log standar...");
                this.logReplayer.replayLogs(criteria);
            }
            
            System.out.println("Proses recovery selesai.");

        } catch (Exception e) {
            System.err.println("Gagal saat proses recovery:");
            e.printStackTrace();
            // TODO: Handle exception (ini adalah error kritis)
        }
    }
}