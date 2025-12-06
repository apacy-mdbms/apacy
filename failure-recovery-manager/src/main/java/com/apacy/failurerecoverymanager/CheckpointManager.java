package com.apacy.failurerecoverymanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.LiteralFactor;
import com.apacy.common.dto.ast.where.BinaryConditionNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.storagemanager.StorageManager;

public class CheckpointManager {

    private final String checkpointDirectory;
    private final LogWriter logWriter;
    private final IStorageManager storageManager;

    public CheckpointManager() {
        this("failure-recovery/checkpoints", null, null);
    }

    public CheckpointManager(String checkpointDirectory) {
        this(checkpointDirectory, null, null);
    }

    public CheckpointManager(String checkpointDirectory, LogWriter logWriter, IStorageManager storageManager) {
        this.checkpointDirectory = checkpointDirectory;
        this.logWriter = logWriter;
        this.storageManager = storageManager;

        try {
            Files.createDirectories(Paths.get(checkpointDirectory));
        } catch (IOException e) {
            System.err.println("Gagal membuat direktori checkpoint: " + e.getMessage());
        }
    }

    public String createCheckpoint() throws IOException {
        String checkpointId = "CP_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("[CheckpointManager] Memulai Checkpoint: " + checkpointId);

        long ts = System.currentTimeMillis();

        // Tulis log checkpoint mulai
        if (logWriter != null) {
            logWriter.writeLog("SYSTEM", "CHECKPOINT_BEGIN", checkpointId, null);
            logWriter.flush();
        }

        int appliedEntries = flushWalToStorage();

        // Buat Flush SM jika ada
        if (storageManager instanceof StorageManager sm) {
            if (sm.getBlockManager() != null) {
                sm.getBlockManager().flush();
            }
        }

        // Save metadata including checkpoint timestamp
        CheckpointInfo info = new CheckpointInfo(
            checkpointId,
            ts,
            0,
            "Checkpoint rutin mDBMS"
        );

        saveCheckpointInfo(info);

        if (logWriter != null) {
            logWriter.writeLog("SYSTEM", "CHECKPOINT_END", checkpointId, null);
            logWriter.flush();
            System.out.println("[CheckpointManager] WAL entries applied during checkpoint: " + appliedEntries);
        }

        System.out.println("[CheckpointManager] Checkpoint berhasil: " + checkpointId);
        return checkpointId;
    }

    private void saveCheckpointInfo(CheckpointInfo info) throws IOException {
        File file = new File(checkpointDirectory, info.checkpointId + ".meta");
        try (FileWriter fw = new FileWriter(file)) {
            fw.write("ID=" + info.checkpointId + "\n");
            fw.write("TIMESTAMP=" + info.timestamp + "\n");
            fw.write("DESC=" + info.description + "\n");
            fw.write("TIME_READABLE=" + Instant.ofEpochMilli(info.timestamp).toString() + "\n");
        }
    }

    public void loadCheckpoint(String checkpointId) throws IOException {
        File file = new File(checkpointDirectory, checkpointId + ".meta");
        if (!file.exists()) {
            throw new IOException("Checkpoint " + checkpointId + " tidak ditemukan.");
        }
        System.out.println("Memuat checkpoint: " + checkpointId);
    }
    
    public List<CheckpointInfo> listCheckpoints() throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(checkpointDirectory))) {
            return paths
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".meta"))
                .map(this::parseCheckpointFile)
                .collect(Collectors.toList());
        }
    }

    // bantu parsing 
    private CheckpointInfo parseCheckpointFile(Path path) {
        String filename = path.getFileName().toString().replace(".meta", "");
        return new CheckpointInfo(filename, 0, 0, "Loaded from file");
    }
    
    public void cleanupOldCheckpoints(int keepCount) throws IOException {
        List<CheckpointInfo> checkpoints = listCheckpoints();
        // Urutkan dari yang terbaru
        checkpoints.sort((a, b) -> b.checkpointId.compareTo(a.checkpointId));

        if (checkpoints.size() > keepCount) {
            for (int i = keepCount; i < checkpoints.size(); i++) {
                String id = checkpoints.get(i).checkpointId;
                Files.deleteIfExists(Paths.get(checkpointDirectory, id + ".meta"));
                System.out.println("Menghapus checkpoint lama: " + id);
            }
        }
    }
    
    public boolean validateCheckpoint(String checkpointId) throws IOException {
        return new File(checkpointDirectory, checkpointId + ".meta").exists();
    }
    
    public CheckpointInfo getCheckpointInfo(String checkpointId) throws IOException {
        if (checkpointId == null) throw new IllegalArgumentException("ID null");
        return new CheckpointInfo(checkpointId, System.currentTimeMillis(), 0, "Info");
    }

    private int flushWalToStorage() {
        if (logWriter == null || storageManager == null) {
            return 0;
        }

        String walPath = logWriter.getLogFilePath();
        if (walPath == null) {
            return 0;
        }

        Path path = Paths.get(walPath);
        if (!Files.exists(path)) {
            return 0;
        }

        int applied = 0;

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                LogEntry entry = LogEntry.fromLogLine(line);
                if (entry == null || !isDataOperation(entry.getOperation())) {
                    continue;
                }

                if (applyEntryToStorage(entry)) {
                    applied++;
                }
            }
        } catch (IOException e) {
            System.err.println("[CheckpointManager] Gagal menerapkan WAL ke storage: " + e.getMessage());
        }

        return applied;
    }

    private boolean applyEntryToStorage(LogEntry entry) {
        String operation = entry.getOperation();
        String tableName = entry.getTableName();
        if (operation == null || tableName == null || "-".equals(tableName)) {
            return false;
        }

        Map<String, Object> afterMap = LogDataParser.toMap(entry.getDataAfter());
        Map<String, Object> beforeMap = LogDataParser.toMap(entry.getDataBefore());

        try {
            switch (operation.toUpperCase()) {
                case "INSERT", "UPDATE" -> {
                    if (afterMap == null || afterMap.isEmpty()) {
                        return false;
                    }
                    storageManager.writeBlock(new DataWrite(tableName, new Row(afterMap), null));
                    return true;
                }
                case "DELETE" -> {
                    Map<String, Object> criteria = (beforeMap != null && !beforeMap.isEmpty()) ? beforeMap : afterMap;
                    if (criteria == null || criteria.isEmpty()) {
                        return false;
                    }
                    WhereConditionNode condition = buildFullMatchCondition(criteria);
                    if (condition != null) {
                        storageManager.deleteBlock(new DataDeletion(tableName, condition));
                    }
                    return true;
                }
                default -> {
                    return false;
                }
            }
        } catch (Exception e) {
            System.err.println("[CheckpointManager] Error saat apply log entry: " + e.getMessage());
            return false;
        }
    }

    private boolean isDataOperation(String operation) {
        return operation != null && (
            "INSERT".equalsIgnoreCase(operation) ||
            "UPDATE".equalsIgnoreCase(operation) ||
            "DELETE".equalsIgnoreCase(operation)
        );
    }
    
    private WhereConditionNode buildFullMatchCondition(Map<String, Object> dataMap) {
        if (dataMap == null || dataMap.isEmpty()) return null;
        
        List<WhereConditionNode> conditions = new ArrayList<>();
        for (Map.Entry<String, Object> e : dataMap.entrySet()) {
            if (e.getValue() != null && !String.valueOf(e.getValue()).equalsIgnoreCase("null")) {
                conditions.add(buildComparison(e.getKey(), "=", e.getValue()));
            }
        }
        
        if (conditions.isEmpty()) return null;
        if (conditions.size() == 1) return conditions.get(0);
        
        WhereConditionNode result = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            result = new BinaryConditionNode(result, "AND", conditions.get(i));
        }
        return result;
    }
    
    private WhereConditionNode buildComparison(String columnName, String operator, Object value) {
        TermNode colTerm = new TermNode(new ColumnFactor(columnName), List.of());
        ExpressionNode leftExpr = new ExpressionNode(colTerm, List.of());
        TermNode valTerm = new TermNode(new LiteralFactor(value), List.of());
        ExpressionNode rightExpr = new ExpressionNode(valTerm, List.of());
        return new ComparisonConditionNode(leftExpr, operator, rightExpr);
    }

    public static class CheckpointInfo {
        public String checkpointId;
        public long timestamp;
        public long size;
        public String description;

        public CheckpointInfo(String checkpointId, long timestamp, long size, String description) {
            this.checkpointId = checkpointId;
            this.timestamp = timestamp;
            this.size = size;
            this.description = description;
        }

        @Override
        public String toString() {
            return "Checkpoint{" + "id='" + checkpointId + '\'' + ", ts=" + timestamp + '}';
        }
    }

    /** Cek apakah direktori checkpoint memiliki file checkpoint */
    public boolean hasCheckpoints() throws IOException {
        Path checkpointPath = Paths.get(checkpointDirectory);
        if (!Files.exists(checkpointPath)) {
            return false;
        }
        try (Stream<Path> paths = Files.list(checkpointPath)) {
            return paths.anyMatch(path -> path.toString().endsWith(".cp"));
        }
    }
}
