package com.apacy.failurerecoverymanager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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

    private static final String TEST_LOG_PATH = "failure-recovery/driver-test/mDBMS.log";
    private static final String TEST_CHECKPOINT_DIR = "failure-recovery/driver-test/checkpoints";

    private static int testsPassed = 0;
    private static int testsFailed = 0;
    private static MockStorageManager mockStorageManager;
    private static FailureRecoveryManager frm;

    public static void main(String[] args) {
        System.out.println("\n=== FRM Driver Program ===\n");

        try {
            initializeTestEnvironment();

            if (args.length > 0 && args[0].equals("--all")) {
                runAllTests();
            } else {
                runInteractiveMenu();
            }
        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }

    private static void runInteractiveMenu() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            System.out.println("\n--- MENU ---");
            System.out.println("1. Jalankan Semua Test");
            System.out.println("2. Test LogWriter");
            System.out.println("3. Test LogEntry");
            System.out.println("4. Test LogReplayer");
            System.out.println("5. Test CheckpointManager");
            System.out.println("6. Test FailureRecoveryManager");
            System.out.println("7. Demo: Transaction Lifecycle");
            System.out.println("8. Demo: Failure Recovery");
            System.out.println("9. Demo: Point-in-Time Recovery");
            System.out.println("10. Keluar");
            System.out.print("Pilih: ");

            String choice = scanner.nextLine().trim();
            System.out.println();

            switch (choice) {
                case "1" -> runAllTests();
                case "2" -> testLogWriter();
                case "3" -> testLogEntry();
                case "4" -> testLogReplayer();
                case "5" -> testCheckpointManager();
                case "6" -> testFailureRecoveryManager();
                case "7" -> demoTransactionLifecycle();
                case "8" -> demoFailureRecovery();
                case "9" -> demoPointInTimeRecovery();
                case "10" -> running = false;
                default -> System.out.println("Pilihan tidak valid");
            }

            if (running && !choice.equals("1")) {
                System.out.print("\nTekan Enter...");
                scanner.nextLine();
            }
        }
        scanner.close();
    }

    private static void runAllTests() {
        testsPassed = 0;
        testsFailed = 0;

        System.out.println("=== RUNNING ALL TESTS ===\n");

        testLogWriter();
        testLogEntry();
        testLogReplayer();
        testCheckpointManager();
        testFailureRecoveryManager();

        System.out.println("\n=== TEST SUMMARY ===");
        System.out.println("Passed: " + testsPassed);
        System.out.println("Failed: " + testsFailed);
        System.out.println("Total:  " + (testsPassed + testsFailed));
    }

    private static void testLogWriter() {
        System.out.println("[LogWriter Tests]");

        try {
            LogWriter lw = new LogWriter(TEST_LOG_PATH);
            pass("LogWriter creation");

            LogEntry entry = new LogEntry("TX1", "INSERT", "table1", null, "Row{data={id=1}}");
            lw.writeLog(entry);
            lw.flush();
            pass("Write log entry");

            lw.rotateLog();
            pass("Log rotation");

            lw.close();
        } catch (Exception e) {
            fail("LogWriter: " + e.getMessage());
        }
    }

    private static void testLogEntry() {
        System.out.println("\n[LogEntry Tests]");

        try {
            LogEntry entry = new LogEntry("TX1", "UPDATE", "table1", "old", "new");
            pass("LogEntry creation: " + entry.getTransactionId());

            String json = entry.toString();
            if (json.contains("\"transactionId\"")) {
                pass("toString JSON format");
            } else {
                fail("toString format invalid");
            }

            LogEntry parsed = LogEntry.fromLogLine(json);
            if (parsed != null && "TX1".equals(parsed.getTransactionId())) {
                pass("fromLogLine parsing");
            } else {
                fail("fromLogLine parsing failed");
            }
        } catch (Exception e) {
            fail("LogEntry: " + e.getMessage());
        }
    }

    private static void testLogReplayer() {
        System.out.println("\n[LogReplayer Tests]");

        try {
            mockStorageManager = new MockStorageManager();
            LogReplayer replayer = new LogReplayer(TEST_LOG_PATH, mockStorageManager);
            pass("LogReplayer creation");

            createTestLog(
                    jsonEntry(1000, "TX1", "BEGIN", "t", null, null),
                    jsonEntry(1001, "TX1", "INSERT", "t", null, "Row{data={id=1}}"),
                    jsonEntry(1002, "TX1", "COMMIT", "t", null, null));

            mockStorageManager = new MockStorageManager();
            replayer = new LogReplayer(TEST_LOG_PATH, mockStorageManager);
            replayer.undoTransaction("TX1");

            if (mockStorageManager.deleteCount > 0) {
                pass("Undo INSERT -> deleteBlock called");
            } else {
                fail("Undo INSERT failed");
            }

            mockStorageManager = new MockStorageManager();
            replayer = new LogReplayer(TEST_LOG_PATH, mockStorageManager);
            replayer.replayLogs(new RecoveryCriteria("FULL_REPLAY", null, null));

            if (mockStorageManager.writeCount > 0) {
                pass("Replay logs -> writeBlock called");
            } else {
                fail("Replay logs failed");
            }
        } catch (Exception e) {
            fail("LogReplayer: " + e.getMessage());
        }
    }

    private static void testCheckpointManager() {
        System.out.println("\n[CheckpointManager Tests]");

        try {
            CheckpointManager cm = new CheckpointManager(TEST_CHECKPOINT_DIR);
            pass("CheckpointManager creation");

            String cpId = cm.createCheckpoint();
            if (cpId != null && !cpId.isEmpty()) {
                pass("Create checkpoint: " + cpId);
            } else {
                fail("Create checkpoint returned null");
            }

            List<CheckpointManager.CheckpointInfo> list = cm.listCheckpoints();
            if (list != null && !list.isEmpty()) {
                pass("List checkpoints: " + list.size() + " found");
            } else {
                fail("List checkpoints empty");
            }
        } catch (Exception e) {
            fail("CheckpointManager: " + e.getMessage());
        }
    }

    private static void testFailureRecoveryManager() {
        System.out.println("\n[FailureRecoveryManager Tests]");

        try {
            FailureRecoveryManager frmDefault = new FailureRecoveryManager();
            pass("Default constructor");

            mockStorageManager = new MockStorageManager();
            frm = new FailureRecoveryManager(mockStorageManager);
            pass("Constructor with StorageManager");

            if (frm.getLogWriter() != null && frm.getLogReplayer() != null) {
                pass("Sub-components initialized");
            } else {
                fail("Sub-components null");
            }

            Row row = new Row(Map.of("id", 1, "name", "Test"));
            ExecutionResult result = new ExecutionResult(true, "OK", 123, "INSERT", 1, List.of(row));
            frm.writeLog(result);
            pass("writeLog");

            frm.writeTransactionLog(999, "BEGIN");
            frm.writeTransactionLog(999, "COMMIT");
            pass("writeTransactionLog");

            frm.saveCheckpoint();
            pass("saveCheckpoint");

            frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", "TX999", null));
            pass("recover UNDO_TRANSACTION");

            frm.shutdown();
            pass("shutdown");
        } catch (Exception e) {
            fail("FRM: " + e.getMessage());
        }
    }

    private static void demoTransactionLifecycle() {
        System.out.println("=== Demo: Transaction Lifecycle ===");

        try {
            mockStorageManager = new MockStorageManager();
            frm = new FailureRecoveryManager(mockStorageManager);

            System.out.println("1. BEGIN TX1000");
            frm.writeTransactionLog(1000, "BEGIN");

            System.out.println("2. INSERT employee Alice");
            Row row = new Row(Map.of("id", 1, "name", "Alice"));
            frm.writeLog(new ExecutionResult(true, "OK", 1000, "INSERT", 1, List.of(row)));

            System.out.println("3. COMMIT TX1000");
            frm.writeTransactionLog(1000, "COMMIT");

            System.out.println("Done! Log written to: failure-recovery/log/mDBMS.log");
            frm.shutdown();
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void demoFailureRecovery() {
        System.out.println("=== Demo: Failure Recovery ===");

        try {
            createTestLog(
                    jsonEntry(5000, "TX_FAIL", "BEGIN", "emp", null, null),
                    jsonEntry(5001, "TX_FAIL", "INSERT", "emp", null, "Row{data={id=1}}"),
                    jsonEntry(5002, "TX_FAIL", "INSERT", "emp", null, "Row{data={id=2}}"));
            System.out.println("Created log with uncommitted transaction TX_FAIL");

            mockStorageManager = new MockStorageManager();
            LogReplayer replayer = new LogReplayer(TEST_LOG_PATH, mockStorageManager);

            System.out.println("Running UNDO for TX_FAIL...");
            replayer.undoTransaction("TX_FAIL");

            System.out.println("Result: " + mockStorageManager.deleteCount + " delete operations");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void demoPointInTimeRecovery() {
        System.out.println("=== Demo: Point-in-Time Recovery ===");

        try {
            long now = System.currentTimeMillis();

            createTestLog(
                    jsonEntry(now, "TX1", "BEGIN", "data", null, null),
                    jsonEntry(now + 1000, "TX1", "INSERT", "data", null, "Row{data={id=1}}"),
                    jsonEntry(now + 2000, "TX1", "COMMIT", "data", null, null),
                    jsonEntry(now + 5000, "TX2", "BEGIN", "data", null, null),
                    jsonEntry(now + 6000, "TX2", "INSERT", "data", null, "Row{data={id=2}}"));
            System.out.println("Created log with TX1 (committed) and TX2 (uncommitted)");

            LocalDateTime cutoff = LocalDateTime.ofInstant(Instant.ofEpochMilli(now + 3000), ZoneOffset.UTC);

            mockStorageManager = new MockStorageManager();
            LogReplayer replayer = new LogReplayer(TEST_LOG_PATH, mockStorageManager);

            System.out.println("Rolling back to cutoff point...");
            replayer.rollbackToTime(new RecoveryCriteria("POINT_IN_TIME", null, cutoff));

            System.out.println("Result: " + mockStorageManager.writeCount + " writes, " +
                    mockStorageManager.deleteCount + " deletes");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void initializeTestEnvironment() throws IOException {
        System.out.println("Initializing...");
        Files.createDirectories(Paths.get(TEST_LOG_PATH).getParent());
        Files.createDirectories(Paths.get(TEST_CHECKPOINT_DIR));
        cleanup();
        mockStorageManager = new MockStorageManager();
    }

    private static void cleanup() {
        try {
            Files.deleteIfExists(Paths.get(TEST_LOG_PATH));
            if (Files.exists(Paths.get(TEST_CHECKPOINT_DIR))) {
                Files.walk(Paths.get(TEST_CHECKPOINT_DIR))
                        .filter(Files::isRegularFile)
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            } catch (IOException e) {
                            }
                        });
            }
            Files.deleteIfExists(Paths.get("failure-recovery/log/mDBMS.log"));
        } catch (IOException e) {
        }
    }

    private static void createTestLog(String... lines) throws IOException {
        Files.createDirectories(Paths.get(TEST_LOG_PATH).getParent());
        Files.write(Paths.get(TEST_LOG_PATH), String.join("\n", lines).getBytes());
    }

    private static String jsonEntry(long ts, String tx, String op, String table, String before, String after) {
        return "{\"timestamp\": " + ts + ", \"transactionId\": \"" + tx + "\", \"operation\": \"" + op +
                "\", \"tableName\": \"" + table + "\", \"dataBefore\": \"" + (before != null ? before : "-") +
                "\", \"dataAfter\": \"" + (after != null ? after : "-") + "\"}";
    }

    private static void pass(String msg) {
        testsPassed++;
        System.out.println("  [PASS] " + msg);
    }

    private static void fail(String msg) {
        testsFailed++;
        System.out.println("  [FAIL] " + msg);
    }

    static class MockStorageManager extends StorageManager {
        int writeCount = 0;
        int deleteCount = 0;

        MockStorageManager() {
            super("driver-test");
        }

        @Override
        public int writeBlock(DataWrite dw) {
            writeCount++;
            System.out.println("    [MOCK] writeBlock: " + dw.tableName());
            return 1;
        }

        @Override
        public int deleteBlock(DataDeletion dd) {
            deleteCount++;
            System.out.println("    [MOCK] deleteBlock: " + dd.tableName());
            return 1;
        }
    }
}
