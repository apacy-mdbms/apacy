package com.apacy.failurerecoverymanager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

            // stop when hit BEGIN for this tx
            if ("BEGIN".equalsIgnoreCase(entry.getOperation())) {
                System.out.println("[LogReplayer] Transaksi " + transactionId + " berhasil di-rollback (undo selesai).");
                return;
            }

            try {
                reverseOperation(entry);
            } catch (Exception e) {
                System.err.println("[ERROR] Gagal melakukan undo: " + entry + ". Error: " + e.getMessage());
            }
        }
    }

    public void rollbackToTime(RecoveryCriteria criteria) throws IOException {
        if (criteria == null || criteria.targetTime() == null) {
            System.err.println("[LogReplayer] Target time tidak disediakan untuk rollback.");
            return;
        }

        if (storageManager == null) {
            System.err.println("[LogReplayer] StorageManager belum terhubung. Tidak bisa rollback.");
            return;
        }

        long cutoffMillis = criteria.targetTime().toInstant(ZoneOffset.UTC).toEpochMilli();
        List<LogEntry> logs = readLogBackwards();

        for (LogEntry entry : logs) {
            if (!isDataOperation(entry.getOperation())) {
                continue;
            }
            if (entry.getTimestamp() < cutoffMillis) {
                break;
            }
            try {
                reverseOperation(entry);
            } catch (Exception e) {
                System.err.println("[LogReplayer] Gagal rollback entry: " + entry + " - " + e.getMessage());
            }
        }
    }

    public void replayLogs(RecoveryCriteria criteria) throws IOException {
        System.out.println("[LogReplayer] Memulai Replay Logs...");

        if (storageManager == null) {
            System.err.println("[LogReplayer] StorageManager belum terhubung. Tidak bisa Replay.");
            return;
        }

        List<LogEntry> logs = readLogForward();

        // cari transaksi yang COMMIT
        Set<String> committedTx = new HashSet<>();
        Map<String, List<LogEntry>> txEntries = new LinkedHashMap<>();

        for (LogEntry e : logs) {
            String tx = e.getTransactionId();
            String op = e.getOperation();
            if (tx != null) {
                txEntries.computeIfAbsent(tx, k -> new ArrayList<>()).add(e);
            }
            if ("COMMIT".equalsIgnoreCase(op)) {
                if (tx != null) committedTx.add(tx);
            }
        }

        // tentukan waktu cutoff jika ada
        Long cutoffMillis = null;
        if (criteria != null && criteria.targetTime() != null) {
            LocalDateTime t = criteria.targetTime();
            cutoffMillis = t.toInstant(ZoneOffset.UTC).toEpochMilli();
        }

        int count = 0;
        // replay hanya transaksi yang COMMIT
        for (LogEntry entry : logs) {
            String tx = entry.getTransactionId();
            String op = entry.getOperation();
            if (!isDataOperation(op)) continue;
            if (tx == null || !committedTx.contains(tx)) continue;

            if (cutoffMillis != null && entry.getTimestamp() > cutoffMillis) {
                // lewati entri yang melewati target waktu
                continue;
            }

            try {
                reapplyOperation(entry);
                count++;
            } catch (Exception e) {
                System.err.println("[LogReplayer] Gagal replay log: " + entry + " - " + e.getMessage());
            }
        }

        System.out.println("[LogReplayer] Replay selesai. " + count + " operasi diaplikasikan ulang.");
    }

    // operasi reverse berdasarkan jenis operasinya
    private void reverseOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation();
        if (operation == null) return;
        String op = operation.toUpperCase();
        String tableName = entry.getTableName();

        if (tableName == null || tableName.equals("-")) return;

        Map<String, Object> beforeMap = LogDataParser.toMap(entry.getDataBefore());
        Map<String, Object> afterMap  = LogDataParser.toMap(entry.getDataAfter());

        switch (op) {
            case "INSERT" -> {
                // Undo INSERT
                if (afterMap == null || afterMap.isEmpty()) {
                    System.err.println("[LogReplayer] No dataAfter for INSERT undo -> skipping.");
                    return;
                }
                System.out.println("   -> [UNDO] Menghapus data yang di-INSERT di tabel " + tableName);
                try {
                    storageManager.deleteBlock(new DataDeletion(tableName, afterMap));
                } catch (Exception e) {
                    System.err.println("      [LogReplayer] StorageManager gagal delete for UNDO INSERT: " + e.getMessage());
                }
            }

            case "DELETE" -> {
                // Undo DELETE
                if (beforeMap == null || beforeMap.isEmpty()) {
                    System.err.println("[LogReplayer] No dataBefore for DELETE undo -> skipping.");
                    return;
                }
                System.out.println("   -> [UNDO] Mengembalikan data DELETE di tabel " + tableName);
                Row rowToRestore = new Row(beforeMap);
                storageManager.writeBlock(new DataWrite(tableName, rowToRestore, null));
            }

            case "UPDATE" -> {
                // Undo UPDATE
                if (beforeMap == null || beforeMap.isEmpty()) {
                    System.err.println("[LogReplayer] No dataBefore for UPDATE undo -> skipping.");
                    return;
                }
                System.out.println("   -> [UNDO] Mengembalikan nilai lama UPDATE di tabel " + tableName);
                Row oldValues = new Row(beforeMap);
                storageManager.writeBlock(new DataWrite(tableName, oldValues, null));
            }

            default -> {
            }
        }
            }

    private void reapplyOperation(LogEntry entry) throws Exception {
        String operation = entry.getOperation();
        if (operation == null) return;
        String op = operation.toUpperCase();
        String tableName = entry.getTableName();

        Map<String, Object> afterMap = LogDataParser.toMap(entry.getDataAfter());
        Map<String, Object> beforeMap = LogDataParser.toMap(entry.getDataBefore());

        switch (op) {
            case "INSERT" -> {
                if (afterMap == null || afterMap.isEmpty()) return;
                storageManager.writeBlock(new DataWrite(tableName, new Row(afterMap), null));
            }
            case "UPDATE" -> {
                Map<String, Object> effectiveAfter = (afterMap != null && !afterMap.isEmpty())
                        ? afterMap
                        : beforeMap;
                if (effectiveAfter == null || effectiveAfter.isEmpty()) return;
                storageManager.writeBlock(new DataWrite(tableName, new Row(effectiveAfter), null));
            }
            case "DELETE" -> {
                if (beforeMap == null || beforeMap.isEmpty()) {
                    // if no beforeMap, try delete by afterMap (maybe contains PK)
                    if (afterMap == null || afterMap.isEmpty()) return;
                    storageManager.deleteBlock(new DataDeletion(tableName, afterMap));
                } else {
                    storageManager.deleteBlock(new DataDeletion(tableName, beforeMap));
                }
            }
            default -> {
            }
        }
    }

    private boolean isDataOperation(String operation) {
        return operation != null && (operation.equalsIgnoreCase("INSERT") || operation.equalsIgnoreCase("UPDATE") || operation.equalsIgnoreCase("DELETE"));
    }

    // read kebalik
    private List<LogEntry> readLogBackwards() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        
        // Cek apakah file ada terlebih dahulu
        if (!Files.exists(Paths.get(logFilePath))) {
            return entries; // Kembalikan list kosong jika file tidak ada
        }
        
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
