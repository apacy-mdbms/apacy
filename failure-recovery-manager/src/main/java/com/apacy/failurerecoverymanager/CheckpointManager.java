package com.apacy.failurerecoverymanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    
    //Menerima dependency dari FailureRecoveryManager
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

        // Flush Write-Ahead Log
        if (logWriter != null) {
            logWriter.writeLog("SYSTEM", "CHECKPOINT_BEGIN", checkpointId, null);
            logWriter.flush();
        }

        // Flush Storage Manager
        if (storageManager instanceof StorageManager) {
            StorageManager sm = (StorageManager) storageManager;
            if (sm.getBlockManager() != null) {
                sm.getBlockManager().flush();
            }
        }

        // Tulis Metadata Checkpoint
        CheckpointInfo info = new CheckpointInfo(
            checkpointId,
            System.currentTimeMillis(),
            0, // Size placeholder
            "Checkpoint rutin mDBMS"
        );

        saveCheckpointInfo(info);

        if (logWriter != null) {
            logWriter.writeLog("SYSTEM", "CHECKPOINT_END", checkpointId, null);
            logWriter.flush();
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

    // Helper parsing
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
}