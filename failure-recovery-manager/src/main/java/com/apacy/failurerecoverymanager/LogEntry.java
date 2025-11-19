package com.apacy.failurerecoverymanager;

public class LogEntry {

    private String transactionId;
    private String operation;
    private String tableName;
    private Object data;
    private long timestamp;  
    public LogEntry(long timestamp, String transactionId, String operation, String tableName, Object data) {
        this.timestamp = timestamp;
        this.transactionId = transactionId;
        this.operation = operation;
        this.tableName = tableName;
        this.data = data;
    }

    public LogEntry(String transactionId, String operation, String tableName, Object data) {
        this(System.currentTimeMillis(), transactionId, operation, tableName, data);
    }

    // Getters
    public String getTransactionId() {
        return transactionId;
    }

    public String getOperation() {
        return operation;
    }

    public String getTableName() {
        return tableName;
    }

    public Object getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    // Setter 
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // Format: timestamp|transactionId|operation|tableName|data
    @Override
    public String toString() {
        String safeData = (data != null ? data.toString().replace("\n", " ").replace("|", " ") : "-");

        return timestamp + "|" +
                (transactionId != null ? transactionId : "-") + "|" +
                (operation != null ? operation : "-") + "|" +
                (tableName != null ? tableName : "-") + "|" +
                safeData;
    }
}
