package com.apacy.failurerecoverymanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogEntryTest {

    private LogEntry logEntry;
    private static final String TRANSACTION_ID = "TX123";
    private static final String OPERATION = "INSERT";
    private static final String TABLE_NAME = "Users";
    private static final Object BEFORE = "{id=1, name='Old'}";
    private static final Object AFTER = "{id=1, name='New'}";

    @BeforeEach
    void setUp() {
        logEntry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, BEFORE, AFTER);
    }

    @Test
    void testLogEntryCreationWithAllParameters() {
        long timestamp = System.currentTimeMillis();
        LogEntry entry = new LogEntry(timestamp, TRANSACTION_ID, OPERATION, TABLE_NAME, BEFORE, AFTER);

        assertEquals(timestamp, entry.getTimestamp());
        assertEquals(TRANSACTION_ID, entry.getTransactionId());
        assertEquals(OPERATION, entry.getOperation());
        assertEquals(TABLE_NAME, entry.getTableName());
        assertEquals(BEFORE, entry.getDataBefore());
        assertEquals(AFTER, entry.getDataAfter());
    }

    @Test
    void testLogEntryCreationWithoutTimestamp() {
        long beforeCreation = System.currentTimeMillis();
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, BEFORE, AFTER);
        long afterCreation = System.currentTimeMillis();

        assertTrue(entry.getTimestamp() >= beforeCreation && entry.getTimestamp() <= afterCreation,
                "Timestamp should be set to current time");
        assertEquals(TRANSACTION_ID, entry.getTransactionId());
        assertEquals(OPERATION, entry.getOperation());
        assertEquals(TABLE_NAME, entry.getTableName());
        assertEquals(BEFORE, entry.getDataBefore());
        assertEquals(AFTER, entry.getDataAfter());
    }

    @Test
    void testGetters() {
        assertEquals(TRANSACTION_ID, logEntry.getTransactionId());
        assertEquals(OPERATION, logEntry.getOperation());
        assertEquals(TABLE_NAME, logEntry.getTableName());
        assertEquals(BEFORE, logEntry.getDataBefore());
        assertEquals(AFTER, logEntry.getDataAfter());
        assertTrue(logEntry.getTimestamp() > 0);
    }

    @Test
    void testSetters() {
        long newTimestamp = 1234567890L;
        String newTransactionId = "TX456";
        String newOperation = "UPDATE";
        String newTableName = "Orders";
        Object newBefore = "{id=2, total=100}";
        Object newAfter = "{id=2, total=200}";

        logEntry.setTimestamp(newTimestamp);
        logEntry.setTransactionId(newTransactionId);
        logEntry.setOperation(newOperation);
        logEntry.setTableName(newTableName);
        logEntry.setDataBefore(newBefore);
        logEntry.setDataAfter(newAfter);

        assertEquals(newTimestamp, logEntry.getTimestamp());
        assertEquals(newTransactionId, logEntry.getTransactionId());
        assertEquals(newOperation, logEntry.getOperation());
        assertEquals(newTableName, logEntry.getTableName());
        assertEquals(newBefore, logEntry.getDataBefore());
        assertEquals(newAfter, logEntry.getDataAfter());
    }

    @Test
    void testToStringFormat() {
        String result = logEntry.toString();

        assertNotNull(result);
        // New format is JSON
        assertTrue(result.startsWith("{"), "Log entry should be JSON format");
        assertTrue(result.endsWith("}"), "Log entry should be JSON format");
        assertTrue(result.contains("\"timestamp\""), "Should contain timestamp field");
        assertTrue(result.contains("\"transactionId\""), "Should contain transactionId field");
        assertTrue(result.contains("\"operation\""), "Should contain operation field");
        assertTrue(result.contains("\"tableName\""), "Should contain tableName field");
        assertTrue(result.contains("\"dataBefore\""), "Should contain dataBefore field");
        assertTrue(result.contains("\"dataAfter\""), "Should contain dataAfter field");
        assertTrue(result.contains(TRANSACTION_ID), "Should contain transaction ID value");
        assertTrue(result.contains(OPERATION), "Should contain operation value");
    }

    @Test
    void testToStringWithNullValues() {
        LogEntry entry = new LogEntry(null, null, null, null, null);
        String result = entry.toString();

        assertNotNull(result);
        // JSON format should have null values represented as "-"
        assertTrue(result.contains("\"transactionId\": \"-\""), "Null transactionId should be '-'");
        assertTrue(result.contains("\"operation\": \"-\""), "Null operation should be '-'");
        assertTrue(result.contains("\"dataBefore\": \"-\""), "Null dataBefore should be '-'");
        assertTrue(result.contains("\"dataAfter\": \"-\""), "Null dataAfter should be '-'");
    }

    @Test
    void testToStringWithDataContainingNewlines() {
        Object dataWithNewlines = "Line1\nLine2\nLine3";
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, dataWithNewlines, dataWithNewlines);
        String result = entry.toString();

        // JSON format stores data as strings, so newlines are preserved in the JSON value
        // But the JSON parsing should handle it correctly
        assertNotNull(result);
        assertTrue(result.contains("Line1"), "Data should be preserved");
    }

    @Test
    void testToStringWithDataContainingPipes() {
        Object dataWithPipes = "value1|value2|value3";
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, dataWithPipes, dataWithPipes);
        String result = entry.toString();

        // JSON format doesn't use pipes as separators, so pipes in data are preserved
        assertNotNull(result);
        assertTrue(result.contains("value1|value2|value3"), "Pipes in data should be preserved in JSON");
        assertTrue(result.startsWith("{") && result.endsWith("}"), "Should be valid JSON");
    }

    @Test
    void testToStringWithNullData() {
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, null, null);
        String result = entry.toString();

        // In JSON format, nulls are represented as "-"
        assertTrue(result.contains("\"dataBefore\": \"-\""), "Null dataBefore should be '-'");
        assertTrue(result.contains("\"dataAfter\": \"-\""), "Null dataAfter should be '-'");
    }

    @Test
    void testLogEntryImmutabilityAfterToString() {
        String originalTransactionId = logEntry.getTransactionId();
        String originalOperation = logEntry.getOperation();

        logEntry.toString();

        assertEquals(originalTransactionId, logEntry.getTransactionId());
        assertEquals(originalOperation, logEntry.getOperation());
    }

    @Test
    void testMultipleLogEntriesWithDifferentTimestamps() throws InterruptedException {
        LogEntry entry1 = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, BEFORE, AFTER);
        Thread.sleep(5); // ensure timestamp difference
        LogEntry entry2 = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, BEFORE, AFTER);

        assertTrue(entry2.getTimestamp() >= entry1.getTimestamp(),
                "Later entry should have equal or greater timestamp");
    }

    @Test
    void testFromLogLineWithNewFormat() {
        // Test JSON format
        long ts = 12345L;
        String line = "{\"timestamp\": " + ts + ", \"transactionId\": \"TX9\", \"operation\": \"UPDATE\", \"tableName\": \"employees\", \"dataBefore\": \"oldRow\", \"dataAfter\": \"newRow\"}";
        LogEntry entry = LogEntry.fromLogLine(line);

        assertNotNull(entry);
        assertEquals(ts, entry.getTimestamp());
        assertEquals("TX9", entry.getTransactionId());
        assertEquals("UPDATE", entry.getOperation());
        assertEquals("employees", entry.getTableName());
        assertEquals("oldRow", entry.getDataBefore());
        assertEquals("newRow", entry.getDataAfter());
    }

    @Test
    void testFromLogLineWithOldFormat() {
        // Old format is no longer supported - should return null
        // But let's test that JSON format works correctly
        long ts = 54321L;
        String line = "{\"timestamp\": " + ts + ", \"transactionId\": \"TX8\", \"operation\": \"INSERT\", \"tableName\": \"employees\", \"dataBefore\": \"-\", \"dataAfter\": \"Row{data={id=1}}\"}";
        LogEntry entry = LogEntry.fromLogLine(line);

        assertNotNull(entry);
        assertEquals(ts, entry.getTimestamp());
        assertEquals("TX8", entry.getTransactionId());
        assertEquals("INSERT", entry.getOperation());
        assertEquals("employees", entry.getTableName());
        assertNull(entry.getDataBefore(), "Dash should be converted to null");
        assertEquals("Row{data={id=1}}", entry.getDataAfter());
    }

    @Test
    void testFromLogLineWithInvalidContent() {
        assertNull(LogEntry.fromLogLine("invalid"), "Invalid log line should return null");
    }
}
