package com.apacy.failurerecoverymanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.Row;
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

        if (logWriter != null) {
            logWriter.writeLog("SYSTEM", "CHECKPOINT_BEGIN", checkpointId, null);
            logWriter.flush();
        }

        int appliedEntries = flushWalToStorage();

        if (storageManager instanceof StorageManager sm) {
            if (sm.getBlockManager() != null) {
                sm.getBlockManager().flush();
            }
        }

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

    private CheckpointInfo parseCheckpointFile(Path path) {
        String filename = path.getFileName().toString().replace(".meta", "");
        return new CheckpointInfo(filename, 0, 0, "Loaded from file");
    }
    
    public void cleanupOldCheckpoints(int keepCount) throws IOException {
        List<CheckpointInfo> checkpoints = listCheckpoints();
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
                    storageManager.deleteBlock(new DataDeletion(tableName, criteria));
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
