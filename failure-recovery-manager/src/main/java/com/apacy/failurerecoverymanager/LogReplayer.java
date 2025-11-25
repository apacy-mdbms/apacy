package com.apacy.failurerecoverymanager;

import com.apacy.common.dto.*;
import com.apacy.storagemanager.StorageManager;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogReplayer {
    
    private final String logFilePath;
    private final StorageManager storageManager;

    public LogReplayer(String logFilePath, StorageManager storageManager) {
        this.logFilePath = logFilePath;
        this.storageManager = storageManager;
    }
    
    // Undo transaksi dari transaction Id
    public void undoTransaction(String transactionId) throws IOException {
        System.out.println("[LogReplayer] Memulai UNDO untuk Transaksi: " + transactionId);
        
        // Validasi Integrasi
        if (storageManager == null) {
            System.err.println("[LogReplayer] StorageManager belum terhubung. Tidak bisa UNDO");
            return;
        }

        List<LogEntry> logs = readLogBackwards();

        for (LogEntry entry : logs) {
            if (!entry.getTransactionId().equals(transactionId)) {
                continue;
            }

            // BEGIN = stop undo
            if ("BEGIN".equalsIgnoreCase(entry.getOperation())) {
                System.out.println("[LogReplayer] Transaksi " + transactionId + " berhasil di-rollback.");
                return;
            }
            
            // reverse
            try {
                reverseOperation(entry);
            } catch (Exception e) {
                System.err.println("[ERROR] Gagal melakukan undo: " + entry + ". Error: " + e.getMessage());
            }
        }
    }

    // Redo transaksi
    public void replayLogs(RecoveryCriteria criteria) throws IOException {
        System.out.println("[LogReplayer] Memulai Replay Logs...");
        
        if (storageManager == null) {
            System.err.println("[LogReplayer] StorageManager belum terhubung. Tidak bisa Replay.");
            return;
        }

        List<LogEntry> logs = readLogForward();
        
        int count = 0;
        for (LogEntry entry : logs) {
            try {
                if (isDataOperation(entry.getOperation())) {
                    reapplyOperation(entry);
                    count++;
                }
            } catch (Exception e) {
                System.err.println("[LogReplayer] Gagal replay log: " + entry);
            }
        }
        System.out.println("[LogReplayer] Replay selesai. " + count + " operasi diaplikasikan ulang.");
    }

    // Main Logic
    private void reverseOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation().toUpperCase();
        String tableName = entry.getTableName();
        
        if (tableName == null || tableName.equals("-") || entry.getData() == null) return;

        Map<String, Object> dataMap = parseDataString(entry.getData().toString());
        if (dataMap.isEmpty()) return;

        switch (operation) {
            case "INSERT":
                System.out.println("   -> [UNDO] Menghapus data INSERTED di tabel " + tableName);
                try {
                    DataDeletion deletion = new DataDeletion(tableName, dataMap);
                    storageManager.deleteBlock(deletion);
                } catch (UnsupportedOperationException e) {
                    System.err.println("      [LogReplayer] StorageManager belum mendukung deleteBlock: " + e.getMessage());
                }
                break;

            case "DELETE":
                System.out.println("   -> [UNDO] Mengembalikan data DELETED di tabel " + tableName);
                Row rowToRestore = new Row(dataMap);
                DataWrite restoreWrite = new DataWrite(tableName, rowToRestore, null);
                storageManager.writeBlock(restoreWrite);
                break;

            case "UPDATE":
                System.out.println("   -> [UNDO] Mengembalikan data ke nilai lama di tabel " + tableName);
                Row oldValues = new Row(dataMap);
                DataWrite updateWrite = new DataWrite(tableName, oldValues, null); 
                storageManager.writeBlock(updateWrite);
                break;
        }
    }

    // REDO
    private void reapplyOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation().toUpperCase();
        String tableName = entry.getTableName();
        Map<String, Object> dataMap = parseDataString(entry.getData().toString());
        
        if (dataMap.isEmpty()) return;

        Row row = new Row(dataMap);

        switch (operation) {
            case "INSERT":
                storageManager.writeBlock(new DataWrite(tableName, row, null));
                break;
            case "UPDATE":
                storageManager.writeBlock(new DataWrite(tableName, row, null));
                break;
            case "DELETE":
                try {
                    storageManager.deleteBlock(new DataDeletion(tableName, dataMap));
                } catch (Exception e) {
                    System.err.println("      [LogReplayer] Gagal menghapus data selama REDO DELETE: " + e.getMessage());
                }
                break;
        }
    }

    private boolean isDataOperation(String operation) {
        return operation != null && (operation.equalsIgnoreCase("INSERT") || operation.equalsIgnoreCase("UPDATE") || operation.equalsIgnoreCase("DELETE"));
    }

    // reader & parser
    private List<LogEntry> readLogBackwards() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        File file = new File(logFilePath);
        if (!file.exists()) return entries;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long fileLength = raf.length();
            StringBuilder sb = new StringBuilder();

            for (long pointer = fileLength - 1; pointer >= 0; pointer--) {
                raf.seek(pointer);
                char c = (char) raf.readByte();

                if (c == '\n') {
                    if (sb.length() > 0) {
                        String line = sb.reverse().toString();
                        LogEntry entry = parseLogLine(line);
                        if (entry != null) entries.add(entry);
                        sb.setLength(0);
                    }
                } else {
                    sb.append(c);
                }
            }
            if (sb.length() > 0) {
                String line = sb.reverse().toString();
                LogEntry entry = parseLogLine(line);
                if (entry != null) entries.add(entry);
            }
        }
        return entries;
    }
    
    private List<LogEntry> readLogForward() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        File file = new File(logFilePath);
        if (!file.exists()) return entries;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // blom integrated with checkpoints
            String line;
            while ((line = raf.readLine()) != null) {
                LogEntry entry = parseLogLine(line);
                if (entry != null) entries.add(entry);
            }
        }
        return entries;
    }

    private LogEntry parseLogLine(String line) {
        try {
            if (line == null || line.trim().isEmpty()) return null;
            String[] parts = line.split("\\|", 5);
            if (parts.length < 5) return null;

            long timestamp = Long.parseLong(parts[0]);
            String txId = parts[1];
            String op = parts[2];
            String table = parts[3];
            String dataStr = parts[4];

            return new LogEntry(timestamp, txId, op, table, dataStr);
        } catch (Exception e) {
            return null;
        }
    }

    // Row{data={key=val}} -> Map<String, Object>
    private Map<String, Object> parseDataString(String dataStr) {
        Map<String, Object> map = new HashMap<>();
        if (dataStr == null || dataStr.equals("-")) return map;

        try {
            String clean = dataStr
                .replace("Row{data=", "")
                .replace("}", "")
                .replace("{", "")
                .replace("[", "")
                .replace("]", "");

            if (clean.trim().isEmpty()) return map;

            String[] pairs = clean.split(",");
            for (String pair : pairs) {
                String[] kv = pair.split("=");
                if (kv.length == 2) {
                    map.put(kv[0].trim(), kv[1].trim());
                }
            }
        } catch (Exception e) {
            System.err.println("Gagal parsing data log: " + dataStr);
        }
        return map;
    }
}