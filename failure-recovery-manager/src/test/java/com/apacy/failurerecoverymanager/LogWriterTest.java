package com.apacy.failurerecoverymanager;

import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class LogWriterTest {

    private static final String TEST_LOG_PATH = "failure-recovery/log/test-logwriter.log";

    @BeforeEach
    void cleanBefore() throws IOException {
        Files.createDirectories(Paths.get("failure-recovery/log"));
        Files.deleteIfExists(Paths.get(TEST_LOG_PATH));
    }

    @Test
    void testLogWriterCreatesFileAndWritesLogEntry() throws Exception {
        LogWriter writer = new LogWriter(TEST_LOG_PATH);

        writer.writeLog("T1", "INSERT", "Users", "{id=1}");
        writer.close();

        File file = new File(TEST_LOG_PATH);

        assertTrue(file.exists(), "Log file should be created");
        assertTrue(file.length() > 0, "Log file should not be empty");

        // Read back the content
        String content = Files.readString(Paths.get(TEST_LOG_PATH));
        assertTrue(content.contains("T1|INSERT|Users"), "Log entry should contain correct values");
    }

    @Test
    void testWriteMultipleLogs() throws Exception {
        LogWriter writer = new LogWriter(TEST_LOG_PATH);

        writer.writeLog("TX10", "UPDATE", "Orders", "{id=99,total=45000}");
        writer.writeLog("TX11", "DELETE", "Items", "{id=7}");
        writer.close();

        String content = Files.readString(Paths.get(TEST_LOG_PATH));

        assertTrue(content.lines().count() >= 2, "Log should contain at least two entries");
    }

    @Test
    void testFlushDoesNotThrow() {
        assertDoesNotThrow(() -> {
            LogWriter writer = new LogWriter(TEST_LOG_PATH);
            writer.writeLog("T1", "INSERT", "Users", "{id=1}");
            writer.flush();
            writer.close();
        });
    }

    @Test
    void testRotateLogCreatesNewFile() throws Exception {
        LogWriter writer = new LogWriter(TEST_LOG_PATH);

        writer.writeLog("T1", "INSERT", "Users", "{id=1}");

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
    }
}
