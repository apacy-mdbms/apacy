package com.apacy.failurerecoverymanager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.storagemanager.StorageManager;

public class FRMDriverProgram {

    private static final String LOG_PATH = "failure-recovery/driver-test/mDBMS.log";
    private static final String CHECKPOINT_DIR = "failure-recovery/driver-test/checkpoints";

    private static MockStorageManager storageManager;
    private static FailureRecoveryManager frm;
    private static Scanner scanner;
    private static int currentTxId = 0;

    public static void main(String[] args) {
        scanner = new Scanner(System.in);

        System.out.println("\n=== FRM Interactive Driver ===");
        System.out.println("Ketik 'help' untuk melihat daftar perintah\n");

        try {
            initialize();
            runCommandLoop();
        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        } finally {
            cleanup();
            scanner.close();
        }
    }

    private static void initialize() throws IOException {
        Files.createDirectories(Paths.get(LOG_PATH).getParent());
        Files.createDirectories(Paths.get(CHECKPOINT_DIR));
        storageManager = new MockStorageManager();
        frm = new FailureRecoveryManager(storageManager);
        System.out.println("[OK] FRM initialized");
    }

    private static void runCommandLoop() {
        while (true) {
            System.out.print("\nfrm> ");
            String input = scanner.nextLine().trim();

            if (input.isEmpty())
                continue;

            String[] parts = input.split("\\s+", 2);
            String cmd = parts[0].toLowerCase();
            String args = parts.length > 1 ? parts[1] : "";

            try {
                switch (cmd) {
                    case "help" -> showHelp();
                    case "begin" -> beginTransaction(args);
                    case "insert" -> doInsert(args);
                    case "update" -> doUpdate(args);
                    case "delete" -> doDelete(args);
                    case "commit" -> commitTransaction(args);
                    case "rollback" -> rollbackTransaction(args);
                    case "checkpoint" -> createCheckpoint();
                    case "recover" -> doRecover(args);
                    case "undo" -> undoTransaction(args);
                    case "showlog" -> showLog();
                    case "showcp" -> showCheckpoints();
                    case "clear" -> clearLog();
                    case "status" -> showStatus();
                    case "exit", "quit" -> {
                        shutdown();
                        return;
                    }
                    default -> System.out.println("Perintah tidak dikenal. Ketik 'help'");
                }
            } catch (Exception e) {
                System.out.println("[ERROR] " + e.getMessage());
            }
        }
    }

    private static void showHelp() {
        System.out.println("""

                === PERINTAH TERSEDIA ===

                TRANSAKSI:
                  begin [txId]              - Mulai transaksi baru
                  commit [txId]             - Commit transaksi
                  rollback [txId]           - Rollback transaksi

                OPERASI DATA:
                  insert <table> <key=value ...>   - Insert data
                  update <table> <key=value ...>   - Update data
                  delete <table> <key=value ...>   - Delete data

                RECOVERY:
                  checkpoint                - Buat checkpoint
                  recover [pit|full]        - Recovery (point-in-time atau full)
                  undo <txId>               - Undo transaksi tertentu

                INFO:
                  showlog                   - Tampilkan isi log file
                  showcp                    - Tampilkan daftar checkpoint
                  status                    - Tampilkan status FRM
                  clear                     - Hapus log file

                LAINNYA:
                  help                      - Tampilkan bantuan ini
                  exit                      - Keluar

                CONTOH:
                  begin TX100
                  insert employees id=1 name=Alice dept=Engineering
                  insert employees id=2 name=Bob dept=Sales
                  commit TX100
                  undo TX100
                """);
    }

    private static void beginTransaction(String args) {
        String txId = args.isEmpty() ? "TX" + (++currentTxId) : args;
        frm.writeTransactionLog(Integer.parseInt(txId.replaceAll("\\D", "")), "BEGIN");
        System.out.println("[OK] BEGIN " + txId);
    }

    private static void doInsert(String args) throws IOException {
        String[] parts = args.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("Usage: insert <table> <key=value ...>");
            return;
        }

        String table = parts[0];
        Map<String, Object> data = parseKeyValues(parts[1]);

        Row row = new Row(data);
        LogEntry entry = new LogEntry(String.valueOf(currentTxId), "INSERT", table, null, row);
        frm.getLogWriter().writeLog(entry);
        frm.getLogWriter().flush();

        System.out.println("[OK] INSERT into " + table + ": " + data);
    }

    private static void doUpdate(String args) throws IOException {
        String[] parts = args.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("Usage: update <table> <key=value ...>");
            return;
        }

        String table = parts[0];
        Map<String, Object> data = parseKeyValues(parts[1]);

        Row row = new Row(data);
        LogEntry entry = new LogEntry(String.valueOf(currentTxId), "UPDATE", table, null, row);
        frm.getLogWriter().writeLog(entry);
        frm.getLogWriter().flush();

        System.out.println("[OK] UPDATE " + table + ": " + data);
    }

    private static void doDelete(String args) throws IOException {
        String[] parts = args.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("Usage: delete <table> <key=value ...>");
            return;
        }

        String table = parts[0];
        Map<String, Object> data = parseKeyValues(parts[1]);

        Row row = new Row(data);
        LogEntry entry = new LogEntry(String.valueOf(currentTxId), "DELETE", table, row, null);
        frm.getLogWriter().writeLog(entry);
        frm.getLogWriter().flush();

        System.out.println("[OK] DELETE from " + table + ": " + data);
    }

    private static void commitTransaction(String args) {
        int txId = args.isEmpty() ? currentTxId : Integer.parseInt(args.replaceAll("\\D", ""));
        frm.writeTransactionLog(txId, "COMMIT");
        System.out.println("[OK] COMMIT TX" + txId);
    }

    private static void rollbackTransaction(String args) {
        int txId = args.isEmpty() ? currentTxId : Integer.parseInt(args.replaceAll("\\D", ""));
        frm.writeTransactionLog(txId, "ROLLBACK");
        System.out.println("[OK] ROLLBACK TX" + txId);
    }

    private static void createCheckpoint() {
        frm.saveCheckpoint();
        System.out.println("[OK] Checkpoint created");
    }

    private static void doRecover(String args) {
        storageManager.reset();

        if (args.equalsIgnoreCase("pit")) {
            System.out.print("Masukkan waktu cutoff (yyyy-MM-dd HH:mm:ss): ");
            String timeStr = scanner.nextLine().trim();
            try {
                LocalDateTime cutoff = LocalDateTime.parse(timeStr,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                frm.recover(new RecoveryCriteria("POINT_IN_TIME", null, cutoff));
            } catch (Exception e) {
                System.out.println("[ERROR] Format waktu salah");
                return;
            }
        } else {
            frm.recover(new RecoveryCriteria("POINT_IN_TIME", null, null));
        }

        System.out.println("[OK] Recovery selesai");
        System.out.println("     Writes: " + storageManager.writeCount);
        System.out.println("     Deletes: " + storageManager.deleteCount);
    }

    private static void undoTransaction(String args) {
        if (args.isEmpty()) {
            System.out.println("Usage: undo <txId>");
            return;
        }

        storageManager.reset();
        frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", args, null));

        System.out.println("[OK] Undo " + args + " selesai");
        System.out.println("     Writes: " + storageManager.writeCount);
        System.out.println("     Deletes: " + storageManager.deleteCount);
    }

    private static void showLog() throws IOException {
        if (!Files.exists(Paths.get(LOG_PATH))) {
            System.out.println("Log file kosong");
            return;
        }

        System.out.println("\n=== LOG FILE ===");
        List<String> lines = Files.readAllLines(Paths.get(LOG_PATH));
        int lineNum = 1;
        for (String line : lines) {
            if (!line.isBlank()) {
                System.out.println(lineNum++ + ": " + line);
            }
        }
        System.out.println("=== " + lines.size() + " entries ===");
    }

    private static void showCheckpoints() throws IOException {
        System.out.println("\n=== CHECKPOINTS ===");
        List<CheckpointManager.CheckpointInfo> cps = frm.getCheckpointManager().listCheckpoints();
        if (cps.isEmpty()) {
            System.out.println("Tidak ada checkpoint");
        } else {
            for (CheckpointManager.CheckpointInfo cp : cps) {
                System.out.println("- " + cp.checkpointId + " (" + cp.description + ")");
            }
        }
    }

    private static void clearLog() throws IOException {
        Files.deleteIfExists(Paths.get(LOG_PATH));
        Files.deleteIfExists(Paths.get("failure-recovery/log/mDBMS.log"));
        System.out.println("[OK] Log cleared");
    }

    private static void showStatus() {
        System.out.println("\n=== STATUS ===");
        System.out.println("Current TX ID: " + currentTxId);
        System.out.println("Log path: " + LOG_PATH);
        System.out.println("Checkpoint dir: " + CHECKPOINT_DIR);
        System.out.println("StorageManager writes: " + storageManager.writeCount);
        System.out.println("StorageManager deletes: " + storageManager.deleteCount);
    }

    private static void shutdown() {
        System.out.println("Shutting down...");
        if (frm != null) {
            frm.shutdown();
        }
        System.out.println("Bye!");
    }

    private static void cleanup() {
        try {
            Files.deleteIfExists(Paths.get(LOG_PATH));
            if (Files.exists(Paths.get(CHECKPOINT_DIR))) {
                Files.walk(Paths.get(CHECKPOINT_DIR))
                        .filter(Files::isRegularFile)
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            } catch (IOException e) {
                            }
                        });
            }
        } catch (IOException e) {
        }
    }

    private static Map<String, Object> parseKeyValues(String input) {
        Map<String, Object> map = new HashMap<>();
        String[] pairs = input.split("\\s+");
        for (String pair : pairs) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                try {
                    map.put(kv[0], Integer.parseInt(kv[1]));
                } catch (NumberFormatException e) {
                    map.put(kv[0], kv[1]);
                }
            }
        }
        return map;
    }

    static class MockStorageManager extends StorageManager {
        int writeCount = 0;
        int deleteCount = 0;

        MockStorageManager() {
            super("driver-test");
        }

        void reset() {
            writeCount = 0;
            deleteCount = 0;
        }

        @Override
        public int writeBlock(DataWrite dw) {
            writeCount++;
            System.out.println("  [SM] writeBlock: " + dw.tableName() + " -> " + dw.newData());
            return 1;
        }

        @Override
        public int deleteBlock(DataDeletion dd) {
            deleteCount++;
            System.out.println("  [SM] deleteBlock: " + dd.tableName() + " -> " + dd.filterCondition());
            return 1;
        }
    }
}
