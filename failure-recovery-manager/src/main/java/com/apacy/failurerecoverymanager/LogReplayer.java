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

            if ("BEGIN".equalsIgnoreCase(entry.getOperation())) {
                System.out.println("[LogReplayer] Transaksi " + transactionId + " berhasil di-rollback (undo selesai).");
                return;
            }

            try {
                reverseOperation(entry);
            } catch (Exception e) {
                System.err.println("[ERROR] Gagal melakukan undo: " + entry + ". Error: " + e.getMessage());
                e.printStackTrace(); // Print stack trace biar kelihatan baris error-nya
            }
        }
    }

    public void rollbackToTime(RecoveryCriteria criteria) throws IOException {
        if (criteria == null || criteria.targetTime() == null) return;
        if (storageManager == null) return;

        long cutoffMillis = criteria.targetTime().toInstant(ZoneOffset.UTC).toEpochMilli();
        List<LogEntry> logs = readLogBackwards();

        for (LogEntry entry : logs) {
            if (!isDataOperation(entry.getOperation())) continue;
            if (entry.getTimestamp() >= cutoffMillis) {
                try {
                    reverseOperation(entry);
                } catch (Exception e) {
                    System.err.println("[LogReplayer] Gagal rollback entry: " + entry + " - " + e.getMessage());
                }
            } else {
                break;
            }
        }
    }

    public void replayLogs(RecoveryCriteria criteria) throws IOException {
        System.out.println("[LogReplayer] Memulai Replay Logs...");
        if (storageManager == null) return;

        List<LogEntry> logs = readLogForward();
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
            if (tx == null || !committedTx.contains(tx)) continue;
            if (cutoffMillis != null && entry.getTimestamp() > cutoffMillis) continue;

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
    private void reverseOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation();
        if (operation == null) return;
        
        String op = operation.toUpperCase();
        String tableName = entry.getTableName();
        if (tableName == null || tableName.equals("-")) return;

        // 1. Parse Data
        Map<String, Object> rawBefore = LogDataParser.toMap(entry.getDataBefore());
        Map<String, Object> rawAfter = LogDataParser.toMap(entry.getDataAfter());

        // 2. NORMALISASI KEY (PENTING! Hapus prefix "tabel." dari nama kolom)
        // Tanpa ini, StorageManager akan return null saat mencari nama kolom
        Map<String, Object> beforeMap = normalizeRowData(rawBefore);
        Map<String, Object> afterMap = normalizeRowData(rawAfter);

        switch (op) {
            case "INSERT" -> {
                if (afterMap == null || afterMap.isEmpty()) return;
                String predicate = buildFullMatchPredicate(afterMap);
                if (!predicate.isEmpty()) {
                    System.out.println("   -> [UNDO INSERT] Deleting inserted row in " + tableName);
                    storageManager.deleteBlock(new DataDeletion(tableName, predicate));
                }
            }

            case "DELETE" -> {
                if (beforeMap == null || beforeMap.isEmpty()) return;
                Map<String, Object> cleanMap = removeNullValues(beforeMap);
                if (!cleanMap.isEmpty()) {
                    System.out.println("   -> [UNDO DELETE] Restoring deleted row in " + tableName);
                    storageManager.writeBlock(new DataWrite(tableName, new Row(cleanMap), null));
                }
            }

            case "UPDATE" -> {
                if (beforeMap == null || beforeMap.isEmpty()) return;
                if (afterMap == null || afterMap.isEmpty()) return;

                System.out.println("   -> [UNDO UPDATE] Reverting update in " + tableName);

                // Step 1: Hapus data yang "salah" (data after)
                String predicate = buildFullMatchPredicate(afterMap);
                if (!predicate.isEmpty()) {
                    try {
                        storageManager.deleteBlock(new DataDeletion(tableName, predicate));
                    } catch (Exception e) {
                        System.err.println("   [Warning] Gagal delete data lama saat undo update. Lanjut restore.");
                    }
                }

                // Step 2: Insert kembali data yang "benar" (data before)
                Map<String, Object> dataToRestore = removeNullValues(beforeMap);
                
                // DEBUGGING BLOCK: Cek apa yang dikirim ke SM
                if (dataToRestore.isEmpty()) {
                    System.err.println("   [ERROR] Data to restore kosong setelah normalisasi!");
                } else {
                    System.out.println("   [DEBUG] Data dikirim ke SM: " + dataToRestore);
                    System.out.println("   [DEBUG] Keys: " + dataToRestore.keySet());
                }

                if (!dataToRestore.isEmpty()) {
                    // Kita kirim sebagai INSERT baru (param ke-3 null)
                    storageManager.writeBlock(new DataWrite(tableName, new Row(dataToRestore), null));
                }
            }
        }
    }

    private void reapplyOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation();
        if (operation == null) return;
        String op = operation.toUpperCase();
        String tableName = entry.getTableName();

        Map<String, Object> rawBefore = LogDataParser.toMap(entry.getDataBefore());
        Map<String, Object> rawAfter = LogDataParser.toMap(entry.getDataAfter());

        Map<String, Object> beforeMap = normalizeRowData(rawBefore);
        Map<String, Object> afterMap = normalizeRowData(rawAfter);

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
     * PENTING: Membersihkan nama kolom dari nama tabel.
     * Contoh: "prodi.id_prodi" -> "id_prodi"
     */
    private Map<String, Object> normalizeRowData(Map<String, Object> originalMap) {
        if (originalMap == null) return null;
        
        Map<String, Object> normalized = new HashMap<>();
        for (Map.Entry<String, Object> entry : originalMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            // Logika pembersihan: Ambil substring setelah titik terakhir
            if (key != null && key.contains(".")) {
                String cleanKey = key.substring(key.lastIndexOf(".") + 1);
                // Hanya masukkan jika belum ada, atau overwrite jika perlu
                normalized.put(cleanKey, value); 
            } else if (key != null) {
                normalized.put(key, value);
            }
        }
        return normalized;
    }

    private String buildFullMatchPredicate(Map<String, Object> dataMap) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Object> e : dataMap.entrySet()) {
            if (e.getValue() != null && !String.valueOf(e.getValue()).equalsIgnoreCase("null")) {
                if (!first) sb.append(" AND ");
                sb.append(e.getKey()).append("='").append(e.getValue()).append("'");
                first = false;
            }
        }
        return sb.toString();
    }

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
                    String line = lineBuffer.reverse().toString();
                    LogEntry entry = LogEntry.fromLogLine(line);
                    if (entry != null) entries.add(entry);
                    lineBuffer.setLength(0);
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