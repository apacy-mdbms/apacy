package com.apacy.failurerecoverymanager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
        assertTrue(result.contains(TRANSACTION_ID));
        assertTrue(result.contains(OPERATION));
        assertTrue(result.contains(TABLE_NAME));
        assertTrue(result.contains(BEFORE.toString()));
        assertTrue(result.contains(AFTER.toString()));

        // Format: timestamp|transactionId|operation|tableName|before|after
        String[] parts = result.split("\\|", -1);
        assertEquals(6, parts.length, "Log entry should have 6 pipe-separated parts");
    }

    @Test
    void testToStringWithNullValues() {
        LogEntry entry = new LogEntry(null, null, null, null, null);
        String result = entry.toString();

        assertNotNull(result);
        long pipeCount = result.chars().filter(ch -> ch == '|').count();
        assertEquals(5, pipeCount, "Format must reserve 6 fields");
        assertTrue(result.contains("-"), "Null values should be replaced with '-'");
    }

    @Test
    void testToStringWithDataContainingNewlines() {
        Object dataWithNewlines = "Line1\nLine2\nLine3";
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, dataWithNewlines, dataWithNewlines);
        String result = entry.toString();

        assertFalse(result.contains("\n"), "Newlines should be replaced with spaces");
        assertTrue(result.contains("Line1 Line2 Line3"));
    }

    @Test
    void testToStringWithDataContainingPipes() {
        Object dataWithPipes = "value1|value2|value3";
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, dataWithPipes, dataWithPipes);
        String result = entry.toString();

        // Exactly five separators should remain (between six fields)
        long pipeCount = result.chars().filter(ch -> ch == '|').count();
        assertEquals(5, pipeCount, "Data pipes should be replaced with spaces");
        assertFalse(result.contains("value1|value2"), "Payload pipes must be sanitized");
    }

    @Test
    void testToStringWithNullData() {
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, null, null);
        String result = entry.toString();

        assertTrue(result.endsWith("-|-"), "Null data should be represented as '-' for both fields");
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
        long ts = 12345L;
        String line = ts + "|TX9|UPDATE|employees|oldRow|newRow";
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
        long ts = 54321L;
        String line = ts + "|TX8|INSERT|employees|Row{data={id=1}}";
        LogEntry entry = LogEntry.fromLogLine(line);

        assertNotNull(entry);
        assertEquals(ts, entry.getTimestamp());
        assertEquals("TX8", entry.getTransactionId());
        assertNull(entry.getDataBefore(), "Old format must map dataBefore to null");
        assertEquals("Row{data={id=1}}", entry.getDataAfter());
    }

    @Test
    void testFromLogLineWithInvalidContent() {
        assertNull(LogEntry.fromLogLine("invalid"), "Invalid log line should return null");
    }
}
