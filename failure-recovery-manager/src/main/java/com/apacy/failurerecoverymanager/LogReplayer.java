package com.apacy.failurerecoverymanager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Row;
import com.apacy.storagemanager.StorageManager;

public class LogReplayer {

    private final String logFilePath;
    private final StorageManager storageManager;

    public LogReplayer(String logFilePath, StorageManager storageManager) {
        this.logFilePath = logFilePath;
        this.storageManager = storageManager;
    }

    // --- PUBLIC METHODS (API) ---

    public void undoTransaction(String transactionId) throws IOException {
        System.out.println("[LogReplayer] Memulai UNDO untuk Transaksi: " + transactionId);

        if (storageManager == null) {
            System.err.println("[LogReplayer] StorageManager belum terhubung. Tidak bisa UNDO");
            return;
        }

        List<LogEntry> logs = readLogBackwards();

        for (LogEntry entry : logs) {
            if (entry.getTransactionId() == null) continue;
            if (!entry.getTransactionId().equals(transactionId)) continue;

            // Stop saat ketemu BEGIN transaksi tersebut
            if ("BEGIN".equalsIgnoreCase(entry.getOperation())) {
                System.out.println("[LogReplayer] Transaksi " + transactionId + " berhasil di-rollback (undo selesai).");
                return;
            }

            try {
                reverseOperation(entry);
            } catch (Exception e) {
                System.err.println("[ERROR] Gagal melakukan undo: " + entry + ". Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void rollbackToTime(RecoveryCriteria criteria) throws IOException {
        if (criteria == null || criteria.targetTime() == null) {
            System.err.println("[LogReplayer] Target time tidak disediakan.");
            return;
        }

        if (storageManager == null) {
            System.err.println("[LogReplayer] StorageManager belum terhubung.");
            return;
        }

        long cutoffMillis = criteria.targetTime().toInstant(ZoneOffset.UTC).toEpochMilli();
        List<LogEntry> logs = readLogBackwards();

        for (LogEntry entry : logs) {
            if (!isDataOperation(entry.getOperation())) continue;
            
            // Jika log lebih baru dari target waktu, kita undo
            if (entry.getTimestamp() >= cutoffMillis) {
                try {
                    reverseOperation(entry);
                } catch (Exception e) {
                    System.err.println("[LogReplayer] Gagal rollback entry: " + entry + " - " + e.getMessage());
                }
            } else {
                // Sudah lewat target waktu, berhenti undo
                break;
            }
        }
    }

    public void replayLogs(RecoveryCriteria criteria) throws IOException {
        System.out.println("[LogReplayer] Memulai Replay Logs...");

        if (storageManager == null) {
            System.err.println("[LogReplayer] StorageManager belum terhubung.");
            return;
        }

        List<LogEntry> logs = readLogForward();
        
        // 1. Identifikasi Transaksi yang COMMIT
        Set<String> committedTx = new HashSet<>();
        for (LogEntry e : logs) {
            if ("COMMIT".equalsIgnoreCase(e.getOperation()) && e.getTransactionId() != null) {
                committedTx.add(e.getTransactionId());
            }
        }

        Long cutoffMillis = null;
        if (criteria != null && criteria.targetTime() != null) {
            cutoffMillis = criteria.targetTime().toInstant(ZoneOffset.UTC).toEpochMilli();
        }

        int count = 0;
        for (LogEntry entry : logs) {
            String tx = entry.getTransactionId();
            String op = entry.getOperation();
            
            if (!isDataOperation(op)) continue;
            if (tx == null || !committedTx.contains(tx)) continue; // Skip uncommitted transactions
            if (cutoffMillis != null && entry.getTimestamp() > cutoffMillis) continue; // Skip future logs

            try {
                reapplyOperation(entry);
                count++;
            } catch (Exception e) {
                System.err.println("[LogReplayer] Gagal replay log: " + entry + " - " + e.getMessage());
            }
        }
        System.out.println("[LogReplayer] Replay selesai. " + count + " operasi diaplikasikan ulang.");
    }

    // --- CORE LOGIC: REVERSE OPERATION (UNDO) ---
    // Strategi: Pecah UPDATE menjadi DELETE + INSERT agar kompatibel dengan SM sederhana
    private void reverseOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation();
        if (operation == null) return;
        
        String op = operation.toUpperCase();
        String tableName = entry.getTableName();
        if (tableName == null || tableName.equals("-")) return;

        Map<String, Object> beforeMap = LogDataParser.toMap(entry.getDataBefore());
        Map<String, Object> afterMap = LogDataParser.toMap(entry.getDataAfter());

        switch (op) {
            // UNDO INSERT -> Lakukan DELETE
            case "INSERT" -> {
                if (afterMap == null || afterMap.isEmpty()) return;
                
                // Gunakan Full Match Predicate agar menghapus baris yang tepat
                String predicate = buildFullMatchPredicate(afterMap);
                if (!predicate.isEmpty()) {
                    System.out.println("   -> [UNDO INSERT] Deleting inserted row in " + tableName);
                    storageManager.deleteBlock(new DataDeletion(tableName, predicate));
                }
            }

            // UNDO DELETE -> Lakukan INSERT
            case "DELETE" -> {
                if (beforeMap == null || beforeMap.isEmpty()) return;
                
                // Masukkan kembali data yang dihapus
                Map<String, Object> cleanMap = removeNullValues(beforeMap);
                if (!cleanMap.isEmpty()) {
                    System.out.println("   -> [UNDO DELETE] Restoring deleted row in " + tableName);
                    // Parameter ke-3 null karena SM menganggap ini Insert biasa
                    storageManager.writeBlock(new DataWrite(tableName, new Row(cleanMap), null));
                }
            }

            // UNDO UPDATE -> Lakukan DELETE (versi salah) + INSERT (versi benar)
            case "UPDATE" -> {
                if (beforeMap == null || beforeMap.isEmpty()) return;
                if (afterMap == null || afterMap.isEmpty()) return;

                System.out.println("   -> [UNDO UPDATE] Reverting update in " + tableName);

                // LANGKAH 1: Hapus versi data saat ini (afterMap)
                String predicate = buildFullMatchPredicate(afterMap);
                if (!predicate.isEmpty()) {
                    storageManager.deleteBlock(new DataDeletion(tableName, predicate));
                }

                // LANGKAH 2: Masukkan kembali versi data lama (beforeMap)
                Map<String, Object> dataToRestore = removeNullValues(beforeMap);
                if (!dataToRestore.isEmpty()) {
                    storageManager.writeBlock(new DataWrite(tableName, new Row(dataToRestore), null));
                }
            }
        }
    }

    // --- CORE LOGIC: REAPPLY OPERATION (REDO) ---
    private void reapplyOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation();
        if (operation == null) return;
        
        String op = operation.toUpperCase();
        String tableName = entry.getTableName();

        Map<String, Object> afterMap = LogDataParser.toMap(entry.getDataAfter());
        Map<String, Object> beforeMap = LogDataParser.toMap(entry.getDataBefore());

        switch (op) {
            case "INSERT" -> {
                if (afterMap != null && !afterMap.isEmpty()) {
                    storageManager.writeBlock(new DataWrite(tableName, new Row(afterMap), null));
                }
            }
            case "DELETE" -> {
                Map<String, Object> target = (beforeMap != null && !beforeMap.isEmpty()) ? beforeMap : afterMap;
                if (target != null && !target.isEmpty()) {
                    String predicate = buildFullMatchPredicate(target);
                    storageManager.deleteBlock(new DataDeletion(tableName, predicate));
                }
            }
            case "UPDATE" -> {
                // REDO UPDATE: Delete Old + Insert New
                if (beforeMap != null && !beforeMap.isEmpty()) {
                    String predicate = buildFullMatchPredicate(beforeMap);
                    storageManager.deleteBlock(new DataDeletion(tableName, predicate));
                }
                if (afterMap != null && !afterMap.isEmpty()) {
                    storageManager.writeBlock(new DataWrite(tableName, new Row(afterMap), null));
                }
            }
        }
    }

    // --- HELPER METHODS ---

    /**
     * Membuat string predicate (SQL-like WHERE clause) berdasarkan pencocokan semua kolom.
     * Digunakan untuk memastikan baris yang dihapus/update adalah benar-benar baris yang dimaksud.
     */
    private String buildFullMatchPredicate(Map<String, Object> dataMap) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Object> e : dataMap.entrySet()) {
            // Kita hanya masukkan yang TIDAK null agar query delete valid dan spesifik
            // String "null" juga diabaikan untuk keamanan
            if (e.getValue() != null && !String.valueOf(e.getValue()).equalsIgnoreCase("null")) {
                if (!first) sb.append(" AND ");
                // Format predicate: key='value'
                sb.append(e.getKey()).append("='").append(e.getValue()).append("'");
                first = false;
            }
        }
        return sb.toString();
    }

    /**
     * Membersihkan Map dari entry bernilai null sebelum dibuat menjadi objek Row.
     * Mencegah NullPointerException di StorageManager.
     */
    private Map<String, Object> removeNullValues(Map<String, Object> map) {
        Map<String, Object> clean = new HashMap<>();
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (e.getValue() != null && !String.valueOf(e.getValue()).equalsIgnoreCase("null")) {
                clean.put(e.getKey(), e.getValue());
            }
        }
        return clean;
    }

    private boolean isDataOperation(String operation) {
        return operation != null && (operation.equalsIgnoreCase("INSERT") || 
               operation.equalsIgnoreCase("UPDATE") || operation.equalsIgnoreCase("DELETE"));
    }

    // --- FILE OPERATIONS ---

    private List<LogEntry> readLogBackwards() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        if (!Files.exists(Paths.get(logFilePath))) return entries;
        
        try (RandomAccessFile raf = new RandomAccessFile(logFilePath, "r")) {
            long fileLength = raf.length();
            long pointer = fileLength - 1;
            StringBuilder lineBuffer = new StringBuilder();
            
            while (pointer >= 0) {
                raf.seek(pointer);
                char c = (char) raf.readByte();
                
                if (c == '\n') {
                    if (lineBuffer.length() > 0) {
                        String line = lineBuffer.reverse().toString();
                        LogEntry entry = LogEntry.fromLogLine(line);
                        if (entry != null) entries.add(entry);
                        lineBuffer.setLength(0);
                    }
                } else {
                    lineBuffer.append(c);
                }
                pointer--;
            }
            
            if (lineBuffer.length() > 0) {
                String line = lineBuffer.reverse().toString();
                LogEntry entry = LogEntry.fromLogLine(line);
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
            String line;
            while ((line = raf.readLine()) != null) {
                LogEntry entry = LogEntry.fromLogLine(line);
                if (entry != null) entries.add(entry);
            }
        }
        return entries;
    }
}