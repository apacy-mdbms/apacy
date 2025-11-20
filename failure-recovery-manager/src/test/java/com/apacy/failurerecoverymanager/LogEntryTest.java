package com.apacy.failurerecoverymanager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {
    
    private LogEntry logEntry;
    private static final String TRANSACTION_ID = "TX123";
    private static final String OPERATION = "INSERT";
    private static final String TABLE_NAME = "Users";
    private static final Object DATA = "{id=1, name='John'}";
    
    @BeforeEach
    void setUp() {
        logEntry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, DATA);
    }
    
    @Test
    void testLogEntryCreationWithAllParameters() {
        long timestamp = System.currentTimeMillis();
        LogEntry entry = new LogEntry(timestamp, TRANSACTION_ID, OPERATION, TABLE_NAME, DATA);
        
        assertEquals(timestamp, entry.getTimestamp());
        assertEquals(TRANSACTION_ID, entry.getTransactionId());
        assertEquals(OPERATION, entry.getOperation());
        assertEquals(TABLE_NAME, entry.getTableName());
        assertEquals(DATA, entry.getData());
    }
    
    @Test
    void testLogEntryCreationWithoutTimestamp() {
        long beforeCreation = System.currentTimeMillis();
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, DATA);
        long afterCreation = System.currentTimeMillis();
        
        assertTrue(entry.getTimestamp() >= beforeCreation && entry.getTimestamp() <= afterCreation,
                "Timestamp should be set to current time");
        assertEquals(TRANSACTION_ID, entry.getTransactionId());
        assertEquals(OPERATION, entry.getOperation());
        assertEquals(TABLE_NAME, entry.getTableName());
        assertEquals(DATA, entry.getData());
    }
    
    @Test
    void testGetters() {
        assertEquals(TRANSACTION_ID, logEntry.getTransactionId());
        assertEquals(OPERATION, logEntry.getOperation());
        assertEquals(TABLE_NAME, logEntry.getTableName());
        assertEquals(DATA, logEntry.getData());
        assertTrue(logEntry.getTimestamp() > 0);
    }
    
    @Test
    void testSetters() {
        long newTimestamp = 1234567890L;
        String newTransactionId = "TX456";
        String newOperation = "UPDATE";
        String newTableName = "Orders";
        Object newData = "{id=2, total=100}";
        
        logEntry.setTimestamp(newTimestamp);
        logEntry.setTransactionId(newTransactionId);
        logEntry.setOperation(newOperation);
        logEntry.setTableName(newTableName);
        logEntry.setData(newData);
        
        assertEquals(newTimestamp, logEntry.getTimestamp());
        assertEquals(newTransactionId, logEntry.getTransactionId());
        assertEquals(newOperation, logEntry.getOperation());
        assertEquals(newTableName, logEntry.getTableName());
        assertEquals(newData, logEntry.getData());
    }
    
    @Test
    void testToStringFormat() {
        String result = logEntry.toString();
        
        assertNotNull(result);
        assertTrue(result.contains(TRANSACTION_ID));
        assertTrue(result.contains(OPERATION));
        assertTrue(result.contains(TABLE_NAME));
        assertTrue(result.contains(DATA.toString()));
        
        // Check format: timestamp|transactionId|operation|tableName|data
        String[] parts = result.split("\\|");
        assertEquals(5, parts.length, "Log entry should have 5 pipe-separated parts");
    }
    
    @Test
    void testToStringWithNullValues() {
        LogEntry entry = new LogEntry(null, null, null, null);
        String result = entry.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("-"), "Null values should be replaced with '-'");
        
        String[] parts = result.split("\\|");
        assertEquals(5, parts.length);
    }
    
    @Test
    void testToStringWithDataContainingNewlines() {
        Object dataWithNewlines = "Line1\nLine2\nLine3";
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, dataWithNewlines);
        String result = entry.toString();
        
        assertFalse(result.contains("\n"), "Newlines should be replaced with spaces");
        assertTrue(result.contains("Line1 Line2 Line3"));
    }
    
    @Test
    void testToStringWithDataContainingPipes() {
        Object dataWithPipes = "value1|value2|value3";
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, dataWithPipes);
        String result = entry.toString();
        
        // Count pipes - should be exactly 4 (the separators), data pipes should be replaced
        long pipeCount = result.chars().filter(ch -> ch == '|').count();
        assertEquals(4, pipeCount, "Data pipes should be replaced with spaces");
    }
    
    @Test
    void testToStringWithNullData() {
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, null);
        String result = entry.toString();
        
        assertTrue(result.endsWith("-"), "Null data should be represented as '-'");
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
        LogEntry entry1 = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, DATA);
        Thread.sleep(10); // Small delay to ensure different timestamps
        LogEntry entry2 = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, DATA);
        
        assertTrue(entry2.getTimestamp() >= entry1.getTimestamp(),
                "Later entry should have equal or greater timestamp");
    }
    
    @Test
    void testLogEntryWithComplexData() {
        Object complexData = new Object() {
            @Override
            public String toString() {
                return "{field1: 'value1', field2: 123, field3: true}";
            }
        };
        
        LogEntry entry = new LogEntry(TRANSACTION_ID, OPERATION, TABLE_NAME, complexData);
        String result = entry.toString();
        
        assertTrue(result.contains("field1"));
        assertTrue(result.contains("value1"));
    }
}
