package com.apacy.failurerecoverymanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.storagemanager.StorageManager;

import java.io.IOException;

public class FailureRecoveryManager extends DBMSComponent implements IFailureRecoveryManager {

    // Konstanta untuk path file log
    private static final String DEFAULT_LOG_PATH = "failure-recovery/log/mDBMS.log";
    private static final String DEFAULT_CHECKPOINT_DIR = "failure-recovery/checkpoints";

    // Gunakan helper class internal yang sudah dibuat
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

        // Inisialisasi helper dengan parameter yang benar
        this.logWriter = new LogWriter(this.logFilePath);

        // LogReplayer membutuhkan StorageManager yang konkret (bukan interface)
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
        // Logika untuk melakukan recovery saat startup jika shutdown sebelumnya tidak
        // normal
        System.out.println(this.getComponentName() + " initialized successfully.");
    }

    @Override
    public void shutdown() {
        System.out.println(this.getComponentName() + " is shutting down...");
        // Selalu buat checkpoint terakhir untuk memastikan data konsisten.
        this.saveCheckpoint();

        // Tutup LogWriter dengan aman
        try {
            if (logWriter != null) {
                logWriter.close();
            }
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
            // Konversi int transactionId ke String
            String transactionId = String.valueOf(info.transactionId());
            String operation = info.operation();

            // ExecutionResult tidak memiliki tableName, gunakan placeholder
            // TODO: Pertimbangkan untuk menambahkan tableName ke ExecutionResult di masa
            // depan
            String tableName = "UNKNOWN_TABLE";

            // Format data dari rows
            Object data = formatDataFromExecutionResult(info);

            // Tulis log
            logWriter.writeLog(transactionId, operation, tableName, data);

        } catch (Exception e) {
            System.err.println("Error writing log: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Menulis log untuk lifecycle transaksi (BEGIN, COMMIT, ROLLBACK)
     */
    public void writeTransactionLog(int transactionId, String lifecycleEvent) {
        try {
            logWriter.writeLog(
                    String.valueOf(transactionId),
                    lifecycleEvent,
                    "-",
                    null);
        } catch (IOException e) {
            System.err.println("Error writing transaction log: " + e.getMessage());
        }
    }

    /**
     * Helper method untuk memformat data dari ExecutionResult
     */
    private Object formatDataFromExecutionResult(ExecutionResult info) {
        if (info.rows() == null || info.rows().isEmpty()) {
            return "-";
        }

        // Untuk operasi yang mengembalikan rows (SELECT), format sebagai list
        // Untuk operasi lain, ambil row pertama jika ada
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
            System.out.println("[FailureRecoveryManager] Starting recovery with type: " + criteria.recoveryType());

            switch (criteria.recoveryType()) {
                case "UNDO_TRANSACTION":
                    if (criteria.transactionId() == null) {
                        System.err.println("Transaction ID is required for UNDO_TRANSACTION recovery.");
                        return;
                    }
                    this.logReplayer.undoTransaction(criteria.transactionId());
                    break;

                case "POINT_IN_TIME":
                    this.logReplayer.replayLogs(criteria);
                    break;

                default:
                    System.out.println("[FailureRecoveryManager] Unknown recovery type, using default replay.");
                    this.logReplayer.replayLogs(criteria);
                    break;
            }

            System.out.println("[FailureRecoveryManager] Recovery completed successfully.");

        } catch (Exception e) {
            System.err.println("A critical error occurred during the recovery process.");
            e.printStackTrace();
        }
    }

    // Getter untuk testing
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