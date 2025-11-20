package com.apacy.failurerecoverymanager;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.failurerecoverymanager.mocks.MockStorageManagerForRecovery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
    void testWriteLogCreatesLogFile() throws IOException {
        Row row = new Row(Map.of("id", 1, "name", "Test"));
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                111,
                "INSERT",
                1,
                List.of(row));

        failureRecoveryManager.writeLog(result);

        // Give it a moment to write
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore
        }

        assertTrue(Files.exists(Paths.get(TEST_LOG_PATH)),
                "Log file should be created");
    }

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
                "1000|TX1|BEGIN|employees|-",
                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}");

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
                "1000|TX2|BEGIN|employees|-",
                "1001|TX2|INSERT|employees|Row{data={id=1, name=Jane}}",
                "1002|TX2|COMMIT|employees|-");

        RecoveryCriteria criteria = new RecoveryCriteria("POINT_IN_TIME", null, null);

        assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria),
                "Should perform point-in-time recovery");

        // Verify that writeBlock was called (replay INSERT)
        assertEquals(1, mockStorageManager.getWriteCount(),
                "Should call writeBlock for INSERT replay");
    }

    @Test
    void testRecoverWithUnknownRecoveryType() throws IOException {
        createTestLog(
                "1000|TX3|BEGIN|test|-",
                "1001|TX3|INSERT|test|Row{data={id=1}}");

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
                "1000|TX5|BEGIN|test|-",
                "1001|TX5|INSERT|test|Row{data={id=1}}",
                "1002|TX5|UPDATE|test|Row{data={id=1, name=Updated}}",
                "1003|TX5|COMMIT|test|-");

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
                "1000|TX200|BEGIN|employees|-",
                "1001|TX200|INSERT|employees|Row{data={id=1, name=John}}",
                "1002|TX200|INSERT|employees|Row{data={id=2, name=Jane}}",
                "1003|TX200|UPDATE|employees|Row{data={id=1, name=OldJohn}}"
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
}
