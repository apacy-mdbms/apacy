package com.apacy.failurerecoverymanager;

import java.io.IOException;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.storagemanager.StorageManager;

public class FailureRecoveryManager extends DBMSComponent implements IFailureRecoveryManager {

    private static final String DEFAULT_LOG_PATH = "failure-recovery/log/mDBMS.log";
    private static final String DEFAULT_CHECKPOINT_DIR = "failure-recovery/checkpoints";

    private final LogWriter logWriter;
    private final LogReplayer logReplayer;
    private final CheckpointManager checkpointManager;
    protected final IStorageManager storageManager;
    private final String logFilePath;

    public FailureRecoveryManager() {
        super("Failure Recovery Manager");
        this.storageManager = null;
        this.logFilePath = DEFAULT_LOG_PATH;
        this.logWriter = new LogWriter(this.logFilePath);
        this.logReplayer = new LogReplayer(this.logFilePath, null);
        this.checkpointManager = new CheckpointManager(DEFAULT_CHECKPOINT_DIR);
    }

    public FailureRecoveryManager(IStorageManager storageManager) {
        super("Failure Recovery Manager");
        this.storageManager = storageManager;
        this.logFilePath = DEFAULT_LOG_PATH;

        this.logWriter = new LogWriter(this.logFilePath);

        StorageManager concreteStorageManager = (storageManager instanceof StorageManager)
                ? (StorageManager) storageManager
                : null;
        this.logReplayer = new LogReplayer(this.logFilePath, concreteStorageManager);

        this.checkpointManager = new CheckpointManager(
                DEFAULT_CHECKPOINT_DIR,
                this.logWriter,
                this.storageManager);
    }

    @Override
    public void initialize() throws Exception {
        System.out.println(this.getComponentName() + " is initializing...");

        // Buat auto recovery jika ada checkpoint/log yang sudah ada
        boolean recoveryNeeded = false;
        if (checkpointManager.hasCheckpoints()) {
            System.out.println("Checkpoint files detected. Recovery may be needed.");
            recoveryNeeded = true;
        } else if (logWriter.hasLogs()) {
            System.out.println("Log files detected. Recovery may be needed.");
            recoveryNeeded = true;
        }

        if (recoveryNeeded) {
            System.out.println("System did not shut down cleanly. Initiating recovery...");
            recover(new RecoveryCriteria("POINT_IN_TIME", null, null));
        }

        System.out.println(this.getComponentName() + " initialized successfully.");
    }

    @Override
    public void shutdown() {
        System.out.println(this.getComponentName() + " is shutting down...");
        this.saveCheckpoint();
        try {
            if (logWriter != null)
                logWriter.close();
        } catch (IOException e) {
            System.err.println("Error closing LogWriter: " + e.getMessage());
        }
        System.out.println(this.getComponentName() + " shut down gracefully.");
    }

    @Override
    public void writeLog(ExecutionResult info) {
        if (info == null) {
            System.err.println("ExecutionResult info is null. Cannot write log.");
            return;
        }

        try {
            String transactionId = String.valueOf(info.transactionId());
            String operation = info.operation();
            String tableName = "UNKNOWN_TABLE";

            Object dataAfter = formatDataFromExecutionResult(info);

            // Buat log entry JSON
            LogEntry entry = new LogEntry(transactionId, operation, tableName, null, dataAfter);
            logWriter.writeLog(entry); // writeLog nerima LogEntry dan dijadiin JSON

        } catch (Exception e) {
            System.err.println("Error writing log: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void writeDataLog(String transactionId, String operation, String tableName, Row dataBefore, Row dataAfter) {
        try {
            LogEntry entry = new LogEntry(transactionId, operation, tableName, dataBefore, dataAfter);
            logWriter.writeLog(entry);
        } catch (IOException e) {
            System.err.println("Error writing data log: " + e.getMessage());
        }
    }

    // Menulis log untuk lifecycle transaksi (BEGIN, COMMIT, ROLLBACK)
    public void writeTransactionLog(int transactionId, String lifecycleEvent) {
        try {
            LogEntry entry = new LogEntry(String.valueOf(transactionId), lifecycleEvent, "-", null, null);
            logWriter.writeLog(entry);
        } catch (IOException e) {
            System.err.println("Error writing transaction log: " + e.getMessage());
        }
    }

    private Object formatDataFromExecutionResult(ExecutionResult info) {
        if (info.rows() == null || info.rows().isEmpty()) {
            return "-";
        }

        if ("SELECT".equalsIgnoreCase(info.operation())) {
            return info.rows();
        } else if (!info.rows().isEmpty()) {
            Row firstRow = info.rows().get(0);
            return firstRow;
        }

        return "-";
    }

    @Override
    public void saveCheckpoint() {
        try {
            System.out.println("Creating a new checkpoint...");
            this.checkpointManager.createCheckpoint();
            // Setelah checkpoint, lakuin rotasi/kompaksi WAL buat ilangin log lama
            if (logWriter != null) {
                try {
                    logWriter.rotateLog(); // simpan log lama
                } catch (IOException e) {
                    System.err.println("[FailureRecoveryManager] Failed to rotate WAL: " + e.getMessage());
                }
            }
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
            System.out.println("[FailureRecoveryManager] Starting recovery with type: " + criteria.recoveryType());

            switch (criteria.recoveryType()) {
                case "UNDO_TRANSACTION" -> {
                    if (criteria.transactionId() == null) {
                        System.err.println("Transaction ID is required for UNDO_TRANSACTION recovery.");
                        return;
                    }
                    this.logReplayer.undoTransaction(criteria.transactionId());
                }

                case "POINT_IN_TIME" -> {
                    if (criteria.targetTime() != null) {
                        this.logReplayer.rollbackToTime(criteria);
                    } else {
                        this.logReplayer.replayLogs(criteria);
                    }
                }

                default -> {
                    System.out.println("[FailureRecoveryManager] Unknown recovery type, using default replay.");
                    this.logReplayer.replayLogs(criteria);
                }
            }

            System.out.println("[FailureRecoveryManager] Recovery completed successfully.");

        } catch (Exception e) {
            System.err.println("A critical error occurred during the recovery process.");
            e.printStackTrace();
        }
    }

    // Getters
    public LogWriter getLogWriter() {
        return logWriter;
    }

    public LogReplayer getLogReplayer() {
        return logReplayer;
    }

    public CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }
}
