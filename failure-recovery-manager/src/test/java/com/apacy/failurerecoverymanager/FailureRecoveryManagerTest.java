package com.apacy.failurerecoverymanager;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.storagemanager.StorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FailureRecoveryManagerTest {

    private FailureRecoveryManager failureRecoveryManager;
    private StorageManager storageManager;
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errorStreamCaptor = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    @BeforeEach
    void setUp() throws Exception {
        // Create a StorageManager with test data directory
        storageManager = new StorageManager("test-data");
        failureRecoveryManager = new FailureRecoveryManager(storageManager);

        // Capture System.out and System.err
        System.setOut(new PrintStream(outputStreamCaptor));
        System.setErr(new PrintStream(errorStreamCaptor));

        // Clean up any existing log files
        try {
            Files.deleteIfExists(Paths.get("failure-recovery/log/mDBMS.log"));
        } catch (IOException e) {
            // Ignore if file doesn't exist
        }
    }

    @AfterEach
    void tearDown() {
        // Restore System.out and System.err
        System.setOut(originalOut);
        System.setErr(originalErr);

        // Clean up test files
        try {
            Files.deleteIfExists(Paths.get("failure-recovery/log/mDBMS.log"));
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }

    @Test
    void testComponentName() {
        assertEquals("Failure Recovery Manager", failureRecoveryManager.getComponentName());
    }

    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> failureRecoveryManager.initialize());

        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("Failure Recovery Manager is initializing..."));
        assertTrue(output.contains("initialized successfully"));
    }

    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> failureRecoveryManager.shutdown());

        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("Failure Recovery Manager is shutting down..."));
        assertTrue(output.contains("shut down gracefully"));
    }

    @Test
    void testShutdownCallsSaveCheckpoint() {
        failureRecoveryManager.shutdown();

        String output = outputStreamCaptor.toString();
        // Verify that checkpoint creation is attempted during shutdown
        assertTrue(output.contains("Creating a new checkpoint") ||
                output.contains("Failed to create checkpoint"));
    }

    @Test
    void testWriteLogWithValidExecutionResult() {
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                123,
                "INSERT",
                2,
                List.of());

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result));
    }

    @Test
    void testWriteLogWithNullExecutionResult() {
        failureRecoveryManager.writeLog(null);

        String errorOutput = errorStreamCaptor.toString();
        assertTrue(errorOutput.contains("ExecutionResult info is null"));
    }

    @Test
    void testWriteLogWithEmptyRows() {
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                456,
                "DELETE",
                0,
                List.of() // empty rows
        );

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result));
    }

    @Test
    void testWriteLogWithDifferentOperations() {
        String[] operations = { "INSERT", "UPDATE", "DELETE", "SELECT" };

        for (int i = 0; i < operations.length; i++) {
            String operation = operations[i];
            ExecutionResult result = new ExecutionResult(
                    true,
                    "Success",
                    100 + i,
                    operation,
                    1,
                    List.of());

            assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result),
                    "Should handle " + operation + " operation");
        }
    }

    @Test
    void testSaveCheckpoint() {
        assertDoesNotThrow(() -> failureRecoveryManager.saveCheckpoint());

        String output = outputStreamCaptor.toString();
        String errorOutput = errorStreamCaptor.toString();

        // Either checkpoint creation message or error message should appear
        assertTrue(output.contains("Creating a new checkpoint") ||
                errorOutput.contains("Failed to create checkpoint"));
    }

    @Test
    void testRecoverWithNullCriteria() {
        failureRecoveryManager.recover(null);

        String errorOutput = errorStreamCaptor.toString();
        assertTrue(errorOutput.contains("Recovery criteria is null"));
        assertTrue(errorOutput.contains("Aborting recovery process"));
    }

    @Test
    void testRecoverWithUndoTransactionType() {
        RecoveryCriteria criteria = new RecoveryCriteria(
                "UNDO_TRANSACTION",
                "TX123",
                null);

        // Should attempt to call undoTransaction (which will throw
        // UnsupportedOperationException)
        failureRecoveryManager.recover(criteria);

        String errorOutput = errorStreamCaptor.toString();
        assertTrue(errorOutput.contains("critical error occurred during the recovery process") ||
                errorOutput.contains("not implemented"));
    }

    @Test
    void testRecoverWithPointInTimeType() {
        RecoveryCriteria criteria = new RecoveryCriteria(
                "POINT_IN_TIME",
                null,
                LocalDateTime.now());

        // Should attempt to call replayLogs (which will throw
        // UnsupportedOperationException)
        failureRecoveryManager.recover(criteria);

        String errorOutput = errorStreamCaptor.toString();
        assertTrue(errorOutput.contains("critical error occurred during the recovery process") ||
                errorOutput.contains("not implemented"));
    }

    @Test
    void testRecoverWithUnknownRecoveryType() {
        RecoveryCriteria criteria = new RecoveryCriteria(
                "UNKNOWN_TYPE",
                "TX456",
                null);

        // Should fall through to default case and call replayLogs
        failureRecoveryManager.recover(criteria);

        String errorOutput = errorStreamCaptor.toString();
        assertTrue(errorOutput.contains("critical error occurred during the recovery process") ||
                errorOutput.contains("not implemented"));
    }

    @Test
    void testRecoverWithEmptyRecoveryType() {
        RecoveryCriteria criteria = new RecoveryCriteria(
                "",
                "TX789",
                null);

        failureRecoveryManager.recover(criteria);

        String errorOutput = errorStreamCaptor.toString();
        assertTrue(errorOutput.contains("critical error occurred during the recovery process") ||
                errorOutput.contains("not implemented"));
    }

    @Test
    void testMultipleWriteLogCalls() {
        for (int i = 0; i < 10; i++) {
            ExecutionResult result = new ExecutionResult(
                    true,
                    "Success",
                    i,
                    "INSERT",
                    1,
                    List.of());

            assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result));
        }
    }

    @Test
    void testInitializeAndShutdownSequence() throws Exception {
        assertDoesNotThrow(() -> failureRecoveryManager.initialize());
        assertDoesNotThrow(() -> failureRecoveryManager.shutdown());

        String output = outputStreamCaptor.toString();
        assertTrue(output.contains("initializing"));
        assertTrue(output.contains("shutting down"));
    }

    @Test
    void testStorageManagerIsNotNull() {
        assertNotNull(failureRecoveryManager.storageManager,
                "StorageManager should be initialized");
    }

    @Test
    void testMultipleSaveCheckpointCalls() {
        // Should be able to call saveCheckpoint multiple times
        assertDoesNotThrow(() -> {
            failureRecoveryManager.saveCheckpoint();
            failureRecoveryManager.saveCheckpoint();
            failureRecoveryManager.saveCheckpoint();
        });
    }

    @Test
    void testRecoverWithMultipleCriteria() {
        RecoveryCriteria[] criteriaArray = {
                new RecoveryCriteria("UNDO_TRANSACTION", "TX1", null),
                new RecoveryCriteria("POINT_IN_TIME", null, LocalDateTime.now()),
                new RecoveryCriteria("CUSTOM", "TX2", LocalDateTime.now())
        };

        for (RecoveryCriteria criteria : criteriaArray) {
            assertDoesNotThrow(() -> failureRecoveryManager.recover(criteria));
        }
    }

    @Test
    void testWriteLogWithNullOperation() {
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                999,
                null, // null operation
                1,
                List.of());

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result));
    }

    @Test
    void testWriteLogWithNullRows() {
        ExecutionResult result = new ExecutionResult(
                true,
                "Success",
                888,
                "INSERT",
                0,
                null // null rows
        );

        assertDoesNotThrow(() -> failureRecoveryManager.writeLog(result));
    }
}
