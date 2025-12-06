package com.apacy.failurerecoverymanager;

public class LogEntry {

    private String transactionId;
    private String operation;
    private String tableName;
    private Object dataBefore; 
    private Object dataAfter; 
    private long timestamp;

    public LogEntry() {
        this.timestamp = System.currentTimeMillis();
    }

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

    // Setters
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public void setOperation(String operation) { this.operation = operation; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public void setDataBefore(Object dataBefore) { this.dataBefore = dataBefore; }
    public void setDataAfter(Object dataAfter) { this.dataAfter = dataAfter; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "{" +
                "\"timestamp\": " + timestamp + ", " +
                "\"transactionId\": \"" + (transactionId != null ? transactionId : "-") + "\", " +
                "\"operation\": \"" + (operation != null ? operation : "-") + "\", " +
                "\"tableName\": \"" + (tableName != null ? tableName : "-") + "\", " +
                "\"dataBefore\": \"" + (dataBefore != null ? dataBefore.toString() : "-") + "\", " +
                "\"dataAfter\": \"" + (dataAfter != null ? dataAfter.toString() : "-") + "\"}";
    }


    //Parse log JSON ke LogEntry
    public static LogEntry fromLogLine(String jsonLine) {
        if (jsonLine == null || jsonLine.trim().isEmpty()) return null;
        
        try {
            jsonLine = jsonLine.trim();
            if (!jsonLine.startsWith("{") || !jsonLine.endsWith("}")) {
                return null;
            }
            
            long ts = System.currentTimeMillis();
            String tx = null;
            String op = null;
            String table = null;
            String before = null;
            String after = null;
            
            // parse json
            String content = jsonLine.substring(1, jsonLine.length() - 1);
            
            int timestampIndex = content.indexOf("\"timestamp\":");
            if (timestampIndex >= 0) {
                int colonIndex = content.indexOf(":", timestampIndex);
                int commaIndex = content.indexOf(",", colonIndex);
                if (commaIndex < 0) commaIndex = content.length();
                String tsStr = content.substring(colonIndex + 1, commaIndex).trim();
                try {
                    ts = Long.parseLong(tsStr);
                } catch (NumberFormatException e) {
                    ts = System.currentTimeMillis();
                }
            }
            
            tx = extractJsonField(content, "transactionId");
            op = extractJsonField(content, "operation");
            table = extractJsonField(content, "tableName");
            before = extractJsonField(content, "dataBefore");
            after = extractJsonField(content, "dataAfter");
            
            tx = "-".equals(tx) ? null : tx;
            op = "-".equals(op) ? null : op;
            table = "-".equals(table) ? null : table;
            before = "-".equals(before) ? null : before;
            after = "-".equals(after) ? null : after;
            
            return new LogEntry(ts, tx, op, table, before, after);
        } catch (Exception e) {
            System.err.println("Failed to parse log line: " + e.getMessage());
            return null;
        }
    }
    
    private static String extractJsonField(String jsonContent, String fieldName) {
        int index = jsonContent.indexOf("\"" + fieldName + "\":");
        if (index < 0) return null;
        
        int colonIndex = jsonContent.indexOf(":", index);
        int startQuote = jsonContent.indexOf("\"", colonIndex) + 1;
        if (startQuote <= colonIndex) return null;
        
        int endQuote = startQuote;
        while (endQuote < jsonContent.length()) {
            if (jsonContent.charAt(endQuote) == '"' && 
                (endQuote == 0 || jsonContent.charAt(endQuote - 1) != '\\')) {
                break;
            }
            endQuote++;
        }
        
        if (endQuote >= jsonContent.length()) return null;
        
        return jsonContent.substring(startQuote, endQuote);
    }
}
