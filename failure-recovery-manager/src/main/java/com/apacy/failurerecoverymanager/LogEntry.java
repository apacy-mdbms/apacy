package com.apacy.failurerecoverymanager;

/**
 * LogEntry extended to store dataBefore (old) and dataAfter (new).
 * toString() uses pipe-separated fields:
 * timestamp|transactionId|operation|tableName|dataBefore|dataAfter
 *
 * dataBefore/dataAfter are serialized via their toString() (Row or "-")
 */
public class LogEntry {

    private String transactionId;
    private String operation;
    private String tableName;
    private Object dataBefore; // old values
    private Object dataAfter;  // new values
    private long timestamp;

    public LogEntry(long timestamp, String transactionId, String operation, String tableName, Object dataBefore, Object dataAfter) {
        this.timestamp = timestamp;
        this.transactionId = transactionId;
        this.operation = operation;
        this.tableName = tableName;
        this.dataBefore = dataBefore;
        this.dataAfter = dataAfter;
    }

    public LogEntry(String transactionId, String operation, String tableName, Object dataBefore, Object dataAfter) {
        this(System.currentTimeMillis(), transactionId, operation, tableName, dataBefore, dataAfter);
    }

    // Getters
    public String getTransactionId() { return transactionId; }
    public String getOperation() { return operation; }
    public String getTableName() { return tableName; }
    public Object getDataBefore() { return dataBefore; }
    public Object getDataAfter() { return dataAfter; }
    public long getTimestamp() { return timestamp; }

    // Setters if needed
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public void setOperation(String operation) { this.operation = operation; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public void setDataBefore(Object dataBefore) { this.dataBefore = dataBefore; }
    public void setDataAfter(Object dataAfter) { this.dataAfter = dataAfter; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    /**
     * Format:
     * timestamp|transactionId|operation|tableName|dataBefore|dataAfter
     *
     * Use '-' for nulls.
     */
    @Override
    public String toString() {
        String safeBefore = (dataBefore != null ? dataBefore.toString().replace("\n"," ").replace("|"," ") : "-");
        String safeAfter  = (dataAfter  != null ? dataAfter.toString().replace("\n"," ").replace("|"," ")  : "-");

        return timestamp + "|" +
                (transactionId != null ? transactionId : "-") + "|" +
                (operation != null ? operation : "-") + "|" +
                (tableName != null ? tableName : "-") + "|" +
                safeBefore + "|" +
                safeAfter;
    }

    /**
     * Backwards-compatible parser for lines written by the new format.
     * If a line has 6 parts, map to the new fields.
     * If it has 5 parts (old format), map dataAfter -> dataAfter and dataBefore = null
     */
    public static LogEntry fromLogLine(String line) {
        if (line == null) return null;
        String[] parts = line.split("\\|", 6);
        try {
            if (parts.length == 6) {
                long ts = Long.parseLong(parts[0]);
                String tx = "-".equals(parts[1]) ? null : parts[1];
                String op = "-".equals(parts[2]) ? null : parts[2];
                String table = "-".equals(parts[3]) ? null : parts[3];
                String before = "-".equals(parts[4]) ? null : parts[4];
                String after  = "-".equals(parts[5]) ? null : parts[5];
                return new LogEntry(ts, tx, op, table, before, after);
            } else if (parts.length == 5) {
                // old format: timestamp|txId|op|table|data
                long ts = Long.parseLong(parts[0]);
                String tx = "-".equals(parts[1]) ? null : parts[1];
                String op = "-".equals(parts[2]) ? null : parts[2];
                String table = "-".equals(parts[3]) ? null : parts[3];
                String data = "-".equals(parts[4]) ? null : parts[4];
                if (op != null && (op.equalsIgnoreCase("DELETE") || op.equalsIgnoreCase("UPDATE"))) {
                    return new LogEntry(ts, tx, op, table, data, null);
                }
                return new LogEntry(ts, tx, op, table, null, data);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }
}
