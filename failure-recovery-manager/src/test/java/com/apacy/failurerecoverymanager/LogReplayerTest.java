package com.apacy.failurerecoverymanager;

import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.DataDeletion;
import com.apacy.failurerecoverymanager.mocks.MockStorageManagerForRecovery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class LogReplayerTest {

        private LogReplayer logReplayer;
        private MockStorageManagerForRecovery mockStorageManager;
        private static final String TEST_LOG_PATH = "failure-recovery/log/test-replayer.log";

        @BeforeEach
        void setUp() throws IOException {
                // Create test directory
                Files.createDirectories(Paths.get("failure-recovery/log"));
                // Clean up any existing test files
                Files.deleteIfExists(Paths.get(TEST_LOG_PATH));

                mockStorageManager = new MockStorageManagerForRecovery();
                logReplayer = new LogReplayer(TEST_LOG_PATH, mockStorageManager);
        }

        @AfterEach
        void tearDown() throws IOException {
                // Clean up test files
                Files.deleteIfExists(Paths.get(TEST_LOG_PATH));
        }

        // ========== Constructor Tests ==========

        @Test
        void testLogReplayerCreationWithValidParameters() {
                assertNotNull(logReplayer, "LogReplayer should be created with valid parameters");
        }

        @Test
        void testLogReplayerCreationWithNullStorageManager() {
                LogReplayer replayer = new LogReplayer(TEST_LOG_PATH, null);
                assertNotNull(replayer, "LogReplayer should be created even with null StorageManager");
        }

        // ========== undoTransaction Tests ==========

        @Test
        void testUndoTransactionWithEmptyLog() throws IOException {
                // Empty log file - should complete without errors
                Files.createFile(Paths.get(TEST_LOG_PATH));

                assertDoesNotThrow(() -> logReplayer.undoTransaction("TX123"),
                                "Should handle empty log file gracefully");

                assertEquals(0, mockStorageManager.getWriteCount(), "No operations should be performed");
                assertEquals(0, mockStorageManager.getDeleteCount(), "No operations should be performed");
        }

        @Test
        void testUndoTransactionWithNullTransactionId() throws IOException {
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}");

                // Should not crash with null transaction ID
                assertDoesNotThrow(() -> logReplayer.undoTransaction(null));
        }

        @Test
        void testUndoInsertOperation() throws IOException {
                // Create log with INSERT operation
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}",
                                "1002|TX1|COMMIT|employees|-");

                logReplayer.undoTransaction("TX1");

                // INSERT undo should call deleteBlock
                assertEquals(1, mockStorageManager.getDeleteCount(),
                                "Should call deleteBlock once for INSERT undo");

                DataDeletion deletion = mockStorageManager.getLastDelete();
                assertNotNull(deletion, "Delete operation should be recorded");
                assertEquals("employees", deletion.tableName(), "Should delete from correct table");
        }

        @Test
        void testUndoDeleteOperation() throws IOException {
                // Create log with DELETE operation
                createTestLog(
                                "1000|TX2|BEGIN|employees|-",
                                "1001|TX2|DELETE|employees|Row{data={id=2, name=Jane}}",
                                "1002|TX2|COMMIT|employees|-");

                logReplayer.undoTransaction("TX2");

                // DELETE undo should call writeBlock to restore data
                assertEquals(1, mockStorageManager.getWriteCount(),
                                "Should call writeBlock once for DELETE undo");

                DataWrite write = mockStorageManager.getLastWrite();
                assertNotNull(write, "Write operation should be recorded");
                assertEquals("employees", write.tableName(), "Should write to correct table");
                assertNotNull(write.newData(), "Should have data to restore");
        }

        @Test
        void testUndoUpdateOperation() throws IOException {
                // Create log with UPDATE operation (old values stored)
                createTestLog(
                                "1000|TX3|BEGIN|employees|-",
                                "1001|TX3|UPDATE|employees|Row{data={id=3, name=OldName}}",
                                "1002|TX3|COMMIT|employees|-");

                logReplayer.undoTransaction("TX3");

                // UPDATE undo should call writeBlock with old values
                assertEquals(1, mockStorageManager.getWriteCount(),
                                "Should call writeBlock once for UPDATE undo");

                DataWrite write = mockStorageManager.getLastWrite();
                assertNotNull(write, "Write operation should be recorded");
                assertEquals("employees", write.tableName(), "Should write to correct table");
        }

        @Test
        void testUndoStopsAtBegin() throws IOException {
                // Create log with multiple operations
                createTestLog(
                                "1000|TX4|BEGIN|employees|-",
                                "1001|TX4|INSERT|employees|Row{data={id=1, name=First}}",
                                "1002|TX4|INSERT|employees|Row{data={id=2, name=Second}}",
                                "1003|TX4|UPDATE|employees|Row{data={id=1, name=OldFirst}}");

                logReplayer.undoTransaction("TX4");

                // Should undo all operations until BEGIN (3 operations: 2 INSERTs + 1 UPDATE)
                // UPDATE undo = 1 write, 2 INSERT undos = 2 deletes
                assertEquals(1, mockStorageManager.getWriteCount(), "Should undo UPDATE");
                assertEquals(2, mockStorageManager.getDeleteCount(), "Should undo 2 INSERTs");
        }

        @Test
        void testUndoWithNonExistentTransactionId() throws IOException {
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}");

                // Should complete without errors even if transaction not found
                assertDoesNotThrow(() -> logReplayer.undoTransaction("TX999"));
                assertEquals(0, mockStorageManager.getWriteCount(), "No operations should be performed");
                assertEquals(0, mockStorageManager.getDeleteCount(), "No operations should be performed");
        }

        @Test
        void testUndoWithNullStorageManager() throws IOException {
                LogReplayer replayerWithNullSM = new LogReplayer(TEST_LOG_PATH, null);
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}");

                // Should return early with error message, not crash
                assertDoesNotThrow(() -> replayerWithNullSM.undoTransaction("TX1"),
                                "Should handle null StorageManager gracefully");
        }

        @Test
        void testUndoSkipsNonDataOperations() throws IOException {
                createTestLog(
                                "1000|TX5|BEGIN|employees|-",
                                "1001|TX5|INSERT|employees|Row{data={id=1, name=John}}",
                                "1002|TX5|COMMIT|employees|-");

                logReplayer.undoTransaction("TX5");

                // Should only undo INSERT, not BEGIN or COMMIT
                assertEquals(1, mockStorageManager.getDeleteCount(), "Should only undo data operations");
        }

        @Test
        void testUndoWithInvalidDataFormat() throws IOException {
                // Log with invalid data format
                createTestLog(
                                "1000|TX6|BEGIN|employees|-",
                                "1001|TX6|INSERT|employees|InvalidDataFormat",
                                "1002|TX6|COMMIT|employees|-");

                // Should handle parsing errors gracefully
                assertDoesNotThrow(() -> logReplayer.undoTransaction("TX6"));
        }

        // ========== replayLogs Tests ==========

        @Test
        void testReplayLogsWithEmptyLog() throws IOException {
                Files.createFile(Paths.get(TEST_LOG_PATH));
                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);

                assertDoesNotThrow(() -> logReplayer.replayLogs(criteria),
                                "Should handle empty log file gracefully");

                assertEquals(0, mockStorageManager.getWriteCount(), "No operations should be performed");
                assertEquals(0, mockStorageManager.getDeleteCount(), "No operations should be performed");
        }

        @Test
        void testReplayInsertOperations() throws IOException {
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}",
                                "1002|TX1|INSERT|employees|Row{data={id=2, name=Jane}}",
                                "1003|TX1|COMMIT|employees|-");

                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);
                logReplayer.replayLogs(criteria);

                // Should replay 2 INSERT operations
                assertEquals(2, mockStorageManager.getWriteCount(),
                                "Should replay 2 INSERT operations");
        }

        @Test
        void testReplayUpdateOperations() throws IOException {
                createTestLog(
                                "1000|TX2|BEGIN|employees|-",
                                "1001|TX2|UPDATE|employees|Row{data={id=1, name=UpdatedName}}",
                                "1002|TX2|COMMIT|employees|-");

                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);
                logReplayer.replayLogs(criteria);

                // Should replay UPDATE operation
                assertEquals(1, mockStorageManager.getWriteCount(),
                                "Should replay UPDATE operation");
        }

        @Test
        void testReplayDeleteOperations() throws IOException {
                createTestLog(
                                "1000|TX3|BEGIN|employees|-",
                                "1001|TX3|DELETE|employees|Row{data={id=1, name=John}}",
                                "1002|TX3|COMMIT|employees|-");

                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);
                logReplayer.replayLogs(criteria);

                // Should replay DELETE operation
                assertEquals(1, mockStorageManager.getDeleteCount(),
                                "Should replay DELETE operation");
        }

        @Test
        void testReplaySkipsNonDataOperations() throws IOException {
                createTestLog(
                                "1000|TX4|BEGIN|employees|-",
                                "1001|TX4|INSERT|employees|Row{data={id=1, name=John}}",
                                "1002|TX4|COMMIT|employees|-",
                                "1003|TX4|CHECKPOINT|-|-");

                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);
                logReplayer.replayLogs(criteria);

                // Should only replay INSERT, not BEGIN/COMMIT/CHECKPOINT
                assertEquals(1, mockStorageManager.getWriteCount(),
                                "Should only replay data operations");
        }

        @Test
        void testReplayWithNullStorageManager() throws IOException {
                LogReplayer replayerWithNullSM = new LogReplayer(TEST_LOG_PATH, null);
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}");

                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);

                // Should return early with error message, not crash
                assertDoesNotThrow(() -> replayerWithNullSM.replayLogs(criteria),
                                "Should handle null StorageManager gracefully");
        }

        @Test
        void testReplayMixedOperations() throws IOException {
                createTestLog(
                                "1000|TX5|BEGIN|employees|-",
                                "1001|TX5|INSERT|employees|Row{data={id=1, name=John}}",
                                "1002|TX5|UPDATE|employees|Row{data={id=1, name=JohnUpdated}}",
                                "1003|TX5|DELETE|employees|Row{data={id=2, name=Jane}}",
                                "1004|TX5|COMMIT|employees|-");

                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);
                logReplayer.replayLogs(criteria);

                // Should replay: 1 INSERT + 1 UPDATE = 2 writes, 1 DELETE = 1 delete
                assertEquals(2, mockStorageManager.getWriteCount(),
                                "Should replay INSERT and UPDATE");
                assertEquals(1, mockStorageManager.getDeleteCount(),
                                "Should replay DELETE");
        }

        @Test
        void testReplayHandlesInvalidLogEntries() throws IOException {
                createTestLog(
                                "1000|TX6|BEGIN|employees|-",
                                "invalid|log|entry", // Invalid entry
                                "1001|TX6|INSERT|employees|Row{data={id=1, name=John}}",
                                "incomplete|entry", // Incomplete entry
                                "1002|TX6|COMMIT|employees|-");

                RecoveryCriteria criteria = new RecoveryCriteria("FULL_REPLAY", null, null);

                // Should skip invalid entries and continue
                assertDoesNotThrow(() -> logReplayer.replayLogs(criteria));
                assertEquals(1, mockStorageManager.getWriteCount(),
                                "Should replay valid INSERT operation");
        }

        // ========== Log Parsing Tests ==========

        @Test
        void testLogParsingWithValidFormat() throws IOException {
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John, salary=50000}}");

                // If parsing works, undo should execute correctly
                assertDoesNotThrow(() -> logReplayer.undoTransaction("TX1"));
        }

        @Test
        void testLogParsingWithEmptyLines() throws IOException {
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "", // Empty line
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}",
                                "" // Empty line
                );

                // Should skip empty lines
                assertDoesNotThrow(() -> logReplayer.undoTransaction("TX1"));
        }

        @Test
        void testDataStringParsingWithComplexData() throws IOException {
                createTestLog(
                                "1000|TX7|BEGIN|employees|-",
                                "1001|TX7|INSERT|employees|Row{data={id=1, name=John Doe, salary=50000, dept=Engineering}}");

                logReplayer.undoTransaction("TX7");

                // Should parse complex data correctly
                assertEquals(1, mockStorageManager.getDeleteCount(),
                                "Should parse and undo complex data");
        }

        @Test
        void testDataStringParsingWithDashValue() throws IOException {
                createTestLog(
                                "1000|TX8|BEGIN|employees|-",
                                "1001|TX8|INSERT|employees|-" // Dash means no data
                );

                logReplayer.undoTransaction("TX8");

                // Should skip entries with "-" as data
                assertEquals(0, mockStorageManager.getDeleteCount(),
                                "Should skip entries with no data");
        }

        // ========== Edge Cases ==========

        @Test
        void testNonExistentLogFile() throws IOException {
                // Don't create the log file
                Files.deleteIfExists(Paths.get(TEST_LOG_PATH));

                // Should handle missing file gracefully
                assertDoesNotThrow(() -> logReplayer.undoTransaction("TX1"));
                assertDoesNotThrow(() -> logReplayer.replayLogs(
                                new RecoveryCriteria("FULL_REPLAY", null, null)));
        }

        @Test
        void testMultipleTransactionsInLog() throws IOException {
                createTestLog(
                                "1000|TX1|BEGIN|employees|-",
                                "1001|TX1|INSERT|employees|Row{data={id=1, name=John}}",
                                "1002|TX1|COMMIT|employees|-",
                                "1003|TX2|BEGIN|departments|-",
                                "1004|TX2|INSERT|departments|Row{data={id=1, name=Engineering}}",
                                "1005|TX2|COMMIT|departments|-");

                // Undo only TX1
                logReplayer.undoTransaction("TX1");

                // Should only undo TX1's INSERT
                assertEquals(1, mockStorageManager.getDeleteCount(),
                                "Should only undo TX1 operations");

                mockStorageManager.clearOperations();

                // Undo TX2
                logReplayer.undoTransaction("TX2");
                assertEquals(1, mockStorageManager.getDeleteCount(),
                                "Should only undo TX2 operations");
        }

        @Test
        void testBackwardReadingOrder() throws IOException {
                createTestLog(
                                "1000|TX9|BEGIN|employees|-",
                                "1001|TX9|INSERT|employees|Row{data={id=1, name=First}}",
                                "1002|TX9|INSERT|employees|Row{data={id=2, name=Second}}",
                                "1003|TX9|INSERT|employees|Row{data={id=3, name=Third}}");

                logReplayer.undoTransaction("TX9");

                // Should undo in reverse order (backward reading)
                // All 3 INSERTs should be undone
                assertEquals(3, mockStorageManager.getDeleteCount(),
                                "Should undo all 3 INSERT operations in reverse order");
        }

        // ========== Helper Methods ==========

        /**
         * Creates a test log file with the given log entries.
         */
        private void createTestLog(String... logLines) throws IOException {
                Files.write(
                                Paths.get(TEST_LOG_PATH),
                                String.join("\n", logLines).getBytes(),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING);
        }
}
