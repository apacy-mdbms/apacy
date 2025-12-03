package com.apacy.failurerecoverymanager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogWriterTest {

    private static final String TEST_LOG_PATH = "failure-recovery/log/test-logwriter.log";

    @BeforeEach
    void cleanBefore() throws IOException {
        Files.createDirectories(Paths.get("failure-recovery/log"));
        Files.deleteIfExists(Paths.get(TEST_LOG_PATH));
    }

    @AfterEach
    void cleanAfter() throws IOException {
        // Clean up test files and any .bak files
        try {
            Files.deleteIfExists(Paths.get(TEST_LOG_PATH));

            // Clean up any .bak files created during rotation tests
            File parent = new File("failure-recovery/log");
            if (parent.exists()) {
                File[] bakFiles = parent
                        .listFiles((dir, name) -> name.contains("test-logwriter.log") && name.endsWith(".bak"));
                if (bakFiles != null) {
                    for (File bakFile : bakFiles) {
                        bakFile.delete();
                    }
                }
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @Test
    void testLogWriterCreatesFileAndWritesLogEntry() throws Exception {
        LogWriter writer = new LogWriter(TEST_LOG_PATH);

        LogEntry entry = new LogEntry("T1", "INSERT", "Users", null, "{id=1}");
        writer.writeLog(entry);
        writer.close();

        File file = new File(TEST_LOG_PATH);

        assertTrue(file.exists(), "Log file should be created");
        assertTrue(file.length() > 0, "Log file should not be empty");

        // Read back the content - should be in JSON format
        String content = Files.readString(Paths.get(TEST_LOG_PATH));
        assertTrue(content.contains("\"transactionId\": \"T1\""), "Log entry should contain transactionId in JSON");
        assertTrue(content.contains("\"operation\": \"INSERT\""), "Log entry should contain operation in JSON");
        assertTrue(content.contains("\"tableName\": \"Users\""), "Log entry should contain tableName in JSON");
    }

    @Test
    void testWriteMultipleLogs() throws Exception {
        LogWriter writer = new LogWriter(TEST_LOG_PATH);

        LogEntry entry1 = new LogEntry("TX10", "UPDATE", "Orders", "{id=99}", "{id=99,total=45000}");
        LogEntry entry2 = new LogEntry("TX11", "DELETE", "Items", "{id=7}", null);
        
        writer.writeLog(entry1);
        writer.writeLog(entry2);
        writer.close();

        String content = Files.readString(Paths.get(TEST_LOG_PATH));

        assertTrue(content.lines().count() >= 2, "Log should contain at least two entries");
        assertTrue(content.contains("\"transactionId\": \"TX10\""), "First entry should be present");
        assertTrue(content.contains("\"transactionId\": \"TX11\""), "Second entry should be present");
    }

    @Test
    void testFlushDoesNotThrow() {
        assertDoesNotThrow(() -> {
            LogWriter writer = new LogWriter(TEST_LOG_PATH);
            LogEntry entry = new LogEntry("T1", "INSERT", "Users", null, "{id=1}");
            writer.writeLog(entry);
            writer.flush();
            writer.close();
        });
    }

    @Test
    void testRotateLogCreatesNewFile() throws Exception {
        LogWriter writer = new LogWriter(TEST_LOG_PATH);

        LogEntry entry = new LogEntry("T1", "INSERT", "Users", null, "{id=1}");
        writer.writeLog(entry);

        writer.rotateLog(); // should rename file and create new empty one

        File rotated = null;

        // Cari file .bak hasil rotasi
        File parent = new File("failure-recovery/log");
        File[] files = parent.listFiles((dir, name) -> name.contains("test-logwriter.log") && name.endsWith(".bak"));

        assertNotNull(files, "Directory listing should not be null");
        assertTrue(files.length > 0, "Rotated .bak file should exist");

        rotated = files[0];

        // Log file baru
        File newLog = new File(TEST_LOG_PATH);

        assertTrue(rotated.exists(), "Rotated backup file must exist");
        assertTrue(newLog.exists(), "New log file must be created after rotation");
        assertEquals(0, newLog.length(), "New log file should be empty after rotation");

        // IMPORTANT: Close the writer to release file handles
        writer.close();
    }
}
