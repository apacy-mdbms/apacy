package com.apacy.failurerecoverymanager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.failurerecoverymanager.mocks.MockStorageManagerForRecovery;

class FailureRecoveryManagerTest {

    private FailureRecoveryManager failureRecoveryManager;
    private MockStorageManagerForRecovery mockStorageManager;
    private static final String TEST_LOG_PATH = "failure-recovery/log/mDBMS.log";
    private static final String TEST_CHECKPOINT_DIR = "failure-recovery/checkpoints";

    @BeforeEach
    void setUp() throws Exception {
        // Create a mock StorageManager for testing
        mockStorageManager = new MockStorageManagerForRecovery();
        failureRecoveryManager = new FailureRecoveryManager(mockStorageManager);

        // Clean up any existing log files and checkpoints
        cleanupTestFiles();
    }

    @AfterEach
    void tearDown() {
        // Clean up test files
        cleanupTestFiles();
    }

    private void cleanupTestFiles() {
        try {
            Files.deleteIfExists(Paths.get(TEST_LOG_PATH));
            // Clean up checkpoint files
            if (Files.exists(Paths.get(TEST_CHECKPOINT_DIR))) {
                Files.walk(Paths.get(TEST_CHECKPOINT_DIR))
                        .filter(Files::isRegularFile)
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                // Ignore
                            }
                        });
            }
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }

    // ========== Component Tests ==========

    @Test
    void testComponentName() {
        assertEquals("Failure Recovery Manager", failureRecoveryManager.getComponentName());
    }

    @Test
    void testDefaultConstructor() {
        FailureRecoveryManager frm = new FailureRecoveryManager();
        assertNotNull(frm, "Should create with default constructor");
        assertNull(frm.storageManager, "Default constructor should have null StorageManager");
    }

    @Test
    void testParameterizedConstructor() {
        assertNotNull(failureRecoveryManager, "Should create with StorageManager");
        assertNotNull(failureRecoveryManager.storageManager, "Should have StorageManager");
        assertEquals(mockStorageManager, failureRecoveryManager.storageManager);
    }

    @Test
    void testComponentsAreInitialized() {
        assertNotNull(failureRecoveryManager.getLogWriter(), "LogWriter should be initialized");
        assertNotNull(failureRecoveryManager.getLogReplayer(), "LogReplayer should be initialized");
        assertNotNull(failureRecoveryManager.getCheckpointManager(), "CheckpointManager should be initialized");
    }

    // ========== Initialize and Shutdown Tests ==========

    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> failureRecoveryManager.initialize(),
                "Initialize should not throw exception");
    }

    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> failureRecoveryManager.shutdown(),
                "Shutdown should not throw exception");
    }

    @Test
    void testInitializeAndShutdownSequence() throws Exception {
        assertDoesNotThrow(() -> {
            failureRecoveryManager.initialize();
            failureRecoveryManager.shutdown();
        }, "Initialize and shutdown sequence should work");
    }

    // ========== writeLog Tests ==========

    @Test
    void testWriteLogWithValidExecutionResult() {
        Row row = new Row(Map.of("id", 1, "name", "John"));
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                123,
                "INSERT",
                1,
                List.of(row));

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result),
                "Should write log without exception");
    }

    @Test
    void testWriteLogWithNullExecutionResult() {
        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(null),
                "Should handle null ExecutionResult gracefully");
    }

    @Test
    void testWriteLogWithEmptyRows() {
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                456,
                "DELETE",
                0,
                List.of());

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result),
                "Should handle empty rows");
    }

    @Test
    void testWriteLogWithNullRows() {
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                789,
                "UPDATE",
                0,
                null);

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result),
                "Should handle null rows");
    }

    @Test
    void testWriteLogWithDifferentOperations() {
        String[] operations = { "INSERT", "UPDATE", "DELETE", "SELECT" };

        for (int i = 0; i < operations.length; i++) {
            String operation = operations[i];
            Row row = new Row(Map.of("id", i, "value", "test" + i));
            ExecutionResult result = new ExecutionResult(
                    true,
                    "Success",
                    100 + i,
                    operation,
                    1,
                    List.of(row));

            assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result),
                    "Should handle " + operation + " operation");
        }
    }

    @Test
    void testWriteLogWithNullOperation() {
        Row row = new Row(Map.of("id", 1));
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                999,
                null,
                1,
                List.of(row));

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result),
                "Should handle null operation");
    }

    @Test
    void testWriteLogPersistsExecutionResultPayload() throws IOException {
        resetWal();

        Row row = new Row(Map.of("id", 5, "name", "Persisted"));
        ExecutionResult result = new ExecutionResult(true, "ok", 500, "INSERT", 1, List.of(row));

        failureRecoveryManager.writeLog(result);
        failureRecoveryManager.getLogWriter().flush();

        LogEntry entry = LogEntry.fromLogLine(readLastLogLine());
        assertNotNull(entry);
        assertEquals("500", entry.getTransactionId());
        assertEquals("INSERT", entry.getOperation());
        assertTrue(entry.getDataAfter().toString().contains("Persisted"), "Payload should be serialized");
    }

    @Test
    void testWriteDataLogPersistsBeforeAndAfterRows() throws IOException {
        resetWal();

        Row before = new Row(Map.of("id", 9, "name", "Old"));
        Row after = new Row(Map.of("id", 9, "name", "New"));

        failureRecoveryManager.writeDataLog("TX900", "UPDATE", "students", before, after);
        failureRecoveryManager.getLogWriter().flush();

        LogEntry entry = LogEntry.fromLogLine(readLastLogLine());
        assertNotNull(entry);
        assertEquals("TX900", entry.getTransactionId());
        assertEquals("UPDATE", entry.getOperation());
        assertTrue(entry.getDataBefore().toString().contains("Old"));
        assertTrue(entry.getDataAfter().toString().contains("New"));
    }

    // @Test
    // void testWriteLogCreatesLogFile() throws IOException {
    // Row row = new Row(Map.of("id", 1, "name", "Test"));
    // ExecutionResult result = new ExecutionResult(
    // true,
    // "Success",
    // 111,
    // "INSERT",
    // 1,
    // List.of(row));
    //
    // failureRecoveryManager.writeLog(result);
    //
    // // Give it a moment to write
    // try {
    // Thread.sleep(100);
    // } catch (InterruptedException e) {
    // // Ignore
    // }
    //
    // // assertTrue(Files.exists(Paths.get(TEST_LOG_PATH)),
    // // "Log file should be created");
    // }

    @Test
    void testMultipleWriteLogCalls() {
        for (int i = 0; i < 10; i++) {
            Row row = new Row(Map.of("id", i, "value", "data" + i));
            ExecutionResult result = new ExecutionResult(
                    true,
                    "Success",
                    i,
                    "INSERT",
                    1,
                    List.of(row));

            assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result),
                    "Should handle multiple write calls");
        }
    }

    // ========== writeTransactionLog Tests ==========

    @Test
    void testWriteTransactionLogBegin() {
        assertDoesNotThrow(() -> failureRecoveryManager.writeTransactionLog(1, "BEGIN"),
                "Should write BEGIN transaction log");
    }

    @Test
    void testWriteTransactionLogCommit() {
        assertDoesNotThrow(() -> failureRecoveryManager.writeTransactionLog(1, "COMMIT"),
                "Should write COMMIT transaction log");
    }

    @Test
    void testWriteTransactionLogRollback() {
        assertDoesNotThrow(() -> failureRecoveryManager.writeTransactionLog(1, "ROLLBACK"),
                "Should write ROLLBACK transaction log");
    }

    @Test
    void testTransactionLifecycle() {
        assertDoesNotThrow(() -> {
            failureRecoveryManager.writeTransactionLog(1, "BEGIN");

            Row row = new Row(Map.of("id", 1, "name", "Test"));
            ExecutionResult result = new ExecutionResult(true, "Success", 1, "INSERT", 1, List.of(row));
            failureRecoveryManager.writeLog(result);

            failureRecoveryManager.writeTransactionLog(1, "COMMIT");
        }, "Should handle full transaction lifecycle");
    }

    // ========== saveCheckpoint Tests ==========

    @Test
    void testSaveCheckpoint() {
        assertDoesNotThrow(() -> failureRecoveryManager.saveCheckpoint(),
                "Should save checkpoint without exception");
    }

    @Test
    void testMultipleSaveCheckpointCalls() {
        assertDoesNotThrow(() -> {
            failureRecoveryManager.saveCheckpoint();
            failureRecoveryManager.saveCheckpoint();
            failureRecoveryManager.saveCheckpoint();
        }, "Should handle multiple checkpoint saves");
    }

    @Test
    void testShutdownCallsSaveCheckpoint() throws IOException {
        failureRecoveryManager.shutdown();

        // Verify checkpoint directory exists
        assertTrue(Files.exists(Paths.get(TEST_CHECKPOINT_DIR)),
                "Checkpoint directory should exist after shutdown");
    }

    // ========== recover Tests ==========

    @Test
    void testRecoverWithNullCriteria() {
        assertDoesNotThrow(() -> failureRecoveryManager.recover(null),
                "Should handle null criteria gracefully");
    }

    @Test
    void testRecoverWithUndoTransactionType() throws IOException {
        // Create a log with transaction to undo
        createTestLog(
                createJsonLogEntry(1000, "TX1", "BEGIN", "employees", null, null),
                createJsonLogEntry(1001, "TX1", "INSERT", "employees", null, "Row{data={id=1, name=John}}"));

        RecoveryCriteria criteria = new RecoveryCriteria("UNDO_TRANSACTION", "TX1", null);

        assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                "Should perform undo recovery");

        // Verify that deleteBlock was called (undo INSERT)
        assertEquals(1, mockStorageManager.getDeleteCount(),
                "Should call deleteBlock for INSERT undo");
    }

    @Test
    void testRecoverWithUndoTransactionTypeNullTransactionId() {
        RecoveryCriteria criteria = new RecoveryCriteria("UNDO_TRANSACTION", null, null);

        assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                "Should handle null transaction ID gracefully");
    }

    @Test
    void testRecoverWithPointInTimeType() throws IOException {
        // Create a log with operations to replay
        createTestLog(
                createJsonLogEntry(1000, "TX2", "BEGIN", "employees", null, null),
                createJsonLogEntry(1001, "TX2", "INSERT", "employees", null, "Row{data={id=1, name=Jane}}"),
                createJsonLogEntry(1002, "TX2", "COMMIT", "employees", null, null));

        RecoveryCriteria criteria = new RecoveryCriteria("POINT_IN_TIME", null, null);

        assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                "Should perform point-in-time recovery");

        // Verify that writeBlock was called (replay INSERT)
        assertEquals(1, mockStorageManager.getWriteCount(),
                "Should call writeBlock for INSERT replay");
    }

    @Test
    void testRecoverPointInTimeWithTargetTimestampRollsBackChanges() throws IOException {
        createTestLog(
                createJsonLogEntry(1000, "TX7", "BEGIN", "employees", null, null),
                createJsonLogEntry(1001, "TX7", "INSERT", "employees", null, "Row{data={id=1, name=Before}}"),
                createJsonLogEntry(1002, "TX7", "UPDATE", "employees", "Row{data={id=1, name=Before}}", "Row{data={id=1, name=After}}"));

        LocalDateTime cutoff = LocalDateTime.ofInstant(Instant.ofEpochMilli(1002), ZoneOffset.UTC);
        RecoveryCriteria criteria = new RecoveryCriteria("POINT_IN_TIME", null, cutoff);

        mockStorageManager.clearOperations();

        assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                "Rollback-to-time should not throw");

        assertEquals(1, mockStorageManager.getWriteCount(),
                "Rollback should restore UPDATE change once");
    }

    @Test
    void testRecoverWithUnknownRecoveryType() throws IOException {
        createTestLog(
                createJsonLogEntry(1000, "TX3", "BEGIN", "test", null, null),
                createJsonLogEntry(1001, "TX3", "INSERT", "test", null, "Row{data={id=1}}"));

        RecoveryCriteria criteria = new RecoveryCriteria("UNKNOWN_TYPE", "TX3", null);

        assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                "Should handle unknown recovery type");
    }

    @Test
    void testRecoverWithEmptyRecoveryType() {
        RecoveryCriteria criteria = new RecoveryCriteria("", "TX4", null);

        assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                "Should handle empty recovery type");
    }

    @Test
    void testRecoverWithMultipleCriteria() throws IOException {
        createTestLog(
                createJsonLogEntry(1000, "TX5", "BEGIN", "test", null, null),
                createJsonLogEntry(1001, "TX5", "INSERT", "test", null, "Row{data={id=1}}"),
                createJsonLogEntry(1002, "TX5", "UPDATE", "test", null, "Row{data={id=1, name=Updated}}"),
                createJsonLogEntry(1003, "TX5", "COMMIT", "test", null, null));

        RecoveryCriteria[] criteriaArray = {
                new RecoveryCriteria("UNDO_TRANSACTION", "TX5", null),
                new RecoveryCriteria("POINT_IN_TIME", null, null)
        };

        for (RecoveryCriteria criteria : criteriaArray) {
            mockStorageManager.clearOperations();
            assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                    "Should handle recovery with criteria: " + criteria.recoveryType());
        }
    }

    // ========== Integration Tests ==========

    @Test
    void testFullTransactionFlowWithRecovery() throws IOException {
        // Write transaction logs
        failureRecoveryManager.writeTransactionLog(100, "BEGIN");

        Row row1 = new Row(Map.of("id", 1, "name", "Alice"));
        ExecutionResult result1 = new ExecutionResult(true, "Success", 100, "INSERT", 1, List.of(row1));
        failureRecoveryManager.writeLog(result1);

        Row row2 = new Row(Map.of("id", 2, "name", "Bob"));
        ExecutionResult result2 = new ExecutionResult(true, "Success", 100, "INSERT", 1, List.of(row2));
        failureRecoveryManager.writeLog(result2);

        failureRecoveryManager.writeTransactionLog(100, "COMMIT");

        // Wait for logs to be written
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore
        }

        // Clear mock operations before recovery to get accurate count
        mockStorageManager.clearOperations();

        // Now perform recovery
        RecoveryCriteria criteria = new RecoveryCriteria("POINT_IN_TIME", null, null);
        failureRecoveryManager.recover(criteria);

        // Verify replay happened
        // assertEquals(2, mockStorageManager.getWriteCount(),
        // "Should replay 2 INSERT operations");
    }

    @Test
    void testUndoTransactionAfterFailure() throws IOException {
        // Simulate a transaction that needs to be undone
        createTestLog(
                createJsonLogEntry(1000, "TX200", "BEGIN", "employees", null, null),
                createJsonLogEntry(1001, "TX200", "INSERT", "employees", null, "Row{data={id=1, name=John}}"),
                createJsonLogEntry(1002, "TX200", "INSERT", "employees", null, "Row{data={id=2, name=Jane}}"),
                createJsonLogEntry(1003, "TX200", "UPDATE", "employees", "Row{data={id=1, name=OldJohn}}", "Row{data={id=1, name=NewJohn}}")
        // No COMMIT - transaction incomplete
        );

        RecoveryCriteria criteria = new RecoveryCriteria("UNDO_TRANSACTION", "TX200", null);
        failureRecoveryManager.recover(criteria);

        // Should undo: 1 UPDATE (writeBlock) + 2 INSERTs (deleteBlock)
        assertEquals(1, mockStorageManager.getWriteCount(), "Should undo UPDATE");
        assertEquals(2, mockStorageManager.getDeleteCount(), "Should undo 2 INSERTs");
    }

    @Test
    void testCheckpointAndRecovery() throws IOException {
        // Write some operations
        failureRecoveryManager.writeTransactionLog(300, "BEGIN");
        Row row = new Row(Map.of("id", 1, "data", "test"));
        ExecutionResult result = new ExecutionResult(true, "Success", 300, "INSERT", 1, List.of(row));
        failureRecoveryManager.writeLog(result);
        failureRecoveryManager.writeTransactionLog(300, "COMMIT");

        // Create checkpoint
        failureRecoveryManager.saveCheckpoint();

        // Verify checkpoint was created
        assertTrue(Files.exists(Paths.get(TEST_CHECKPOINT_DIR)),
                "Checkpoint directory should exist");
    }

    // ========== Helper Methods ==========

    /**
     * Creates a test log file with the given log entries.
     */
    private void createTestLog(String... logLines) throws IOException {
        Files.createDirectories(Paths.get(TEST_LOG_PATH).getParent());
        Files.write(
                Paths.get(TEST_LOG_PATH),
                String.join("\n", logLines).getBytes());
    }

    private void resetWal() throws IOException {
        // Simple approach: truncate the log file to empty it out
        Path logPath = Paths.get(TEST_LOG_PATH);
        Files.createDirectories(logPath.getParent());
        // Truncate the file to 0 bytes if it exists
        try (var channel = java.nio.channels.FileChannel.open(logPath,
                java.nio.file.StandardOpenOption.WRITE,
                java.nio.file.StandardOpenOption.CREATE)) {
            channel.truncate(0);
        } catch (IOException e) {
            // If file is locked, just continue - the test will work with clean state
        }
    }

    private String readLastLogLine() throws IOException {
        Path path = Paths.get(TEST_LOG_PATH);
        if (!Files.exists(path)) {
            return null;
        }
        List<String> lines = Files.readAllLines(path);
        for (int i = lines.size() - 1; i >= 0; i--) {
            String line = lines.get(i);
            if (line != null && !line.isBlank()) {
                return line;
            }
        }
        return null;
    }

    private String createJsonLogEntry(long timestamp, String transactionId, String operation,
                                      String tableName, String dataBefore, String dataAfter) {
        return "{" +
                "\"timestamp\": " + timestamp + ", " +
                "\"transactionId\": \"" + transactionId + "\", " +
                "\"operation\": \"" + operation + "\", " +
                "\"tableName\": \"" + tableName + "\", " +
                "\"dataBefore\": \"" + (dataBefore != null ? dataBefore : "-") + "\", " +
                "\"dataAfter\": \"" + (dataAfter != null ? dataAfter : "-") + "\"}";
    }
}
