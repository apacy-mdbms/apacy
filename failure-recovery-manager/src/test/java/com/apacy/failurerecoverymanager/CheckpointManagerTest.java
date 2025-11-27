package com.apacy.failurerecoverymanager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;
import com.apacy.failurerecoverymanager.mocks.MockStorageManagerForRecovery;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// testing
class CheckpointManagerTest {

    private CheckpointManager checkpointManager;
    private static final String TEST_DIR = "failure-recovery/test-checkpoints";

    @BeforeEach
    void setUp() throws IOException {
        // 1. Bersihkan folder test sebelum mulai
        deleteDirectory(new File(TEST_DIR));
        Files.createDirectories(Paths.get(TEST_DIR));
        
        // 2. Inisialisasi CheckpointManager
        // Kita pass 'null' untuk LogWriter dan StorageManager karena di Unit Test ini
        // kita hanya ingin menguji apakah FILE CHECKPOINT (.meta) berhasil dibuat/dihapus.
        checkpointManager = new CheckpointManager(TEST_DIR, null, null);
    }

    @AfterEach
    void tearDown() {
        // Bersihkan setelah selesai
        deleteDirectory(new File(TEST_DIR));
    }

    // Helper untuk hapus folder rekursif
    private void deleteDirectory(File directory) {
        if (!directory.exists()) return;
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }

    @Test
    void testCreateCheckpoint_ShouldCreateFile() throws IOException {
        // ACT: Panggil createCheckpoint
        String cpId = checkpointManager.createCheckpoint();
        
        // ASSERT: Pastikan ID tidak null
        assertNotNull(cpId, "Checkpoint ID tidak boleh null");
        System.out.println("Created Checkpoint ID: " + cpId);

        // ASSERT: Pastikan file .meta terbentuk di disk
        File metaFile = new File(TEST_DIR, cpId + ".meta");
        assertTrue(metaFile.exists(), "File metadata checkpoint (.meta) harusnya terbentuk di disk");
        assertTrue(metaFile.length() > 0, "File metadata tidak boleh kosong");
    }

    @Test
    void testListCheckpoints() throws IOException {
        // ARRANGE: Buat 2 checkpoint
        String id1 = checkpointManager.createCheckpoint();
        try { Thread.sleep(10); } catch (Exception e) {} // Jeda biar timestamp beda
        String id2 = checkpointManager.createCheckpoint();

        // ACT: Ambil daftar
        List<CheckpointManager.CheckpointInfo> list = checkpointManager.listCheckpoints();
        
        // ASSERT
        assertNotNull(list);
        assertEquals(2, list.size(), "Harusnya ada 2 checkpoint yang terdaftar");
        
        // Cek apakah ID yang kita buat ada di dalam list
        boolean found1 = list.stream().anyMatch(c -> c.checkpointId.equals(id1));
        boolean found2 = list.stream().anyMatch(c -> c.checkpointId.equals(id2));
        assertTrue(found1 && found2, "Semua checkpoint yang dibuat harus muncul di list");
    }

    @Test
    void testCleanupOldCheckpoints() throws IOException {
        // ARRANGE: Buat 3 checkpoint
        checkpointManager.createCheckpoint(); // Lama
        try { Thread.sleep(10); } catch (Exception e) {}
        checkpointManager.createCheckpoint(); // Tengah
        try { Thread.sleep(10); } catch (Exception e) {}
        String newestId = checkpointManager.createCheckpoint(); // Baru

        // ACT: Minta simpan cuma 1 yang paling baru
        checkpointManager.cleanupOldCheckpoints(1);
        
        // ASSERT
        List<CheckpointManager.CheckpointInfo> list = checkpointManager.listCheckpoints();
        assertEquals(1, list.size(), "Harusnya sisa 1 checkpoint saja");
        assertEquals(newestId, list.get(0).checkpointId, "Yang tersisa harusnya checkpoint paling baru");
    }

    @Test
    void testCheckpointFlushesWalEntriesToStorage() throws Exception {
        String walPath = TEST_DIR + "/wal.log";
        MockStorageManagerForRecovery mockStorage = new MockStorageManagerForRecovery();
        LogWriter writer = new LogWriter(walPath);

        try {
            // tulis dua operasi data ke WAL
            LogEntry insertEntry = new LogEntry("TX1", "INSERT", "students", null, new Row(Map.of("id", 1, "name", "Alice")));
            LogEntry deleteEntry = new LogEntry("TX2", "DELETE", "students", new Row(Map.of("id", 2, "name", "Bob")), null);
            writer.writeLog(insertEntry);
            writer.writeLog(deleteEntry);

            CheckpointManager manager = new CheckpointManager(TEST_DIR, writer, mockStorage);
            manager.createCheckpoint();

            assertEquals(1, mockStorage.getWriteCount(), "INSERT harus diterapkan ke storage");
            assertEquals(1, mockStorage.getDeleteCount(), "DELETE harus diterapkan ke storage");
        } finally {
            writer.close();
            Files.deleteIfExists(Paths.get(walPath));
        }
    }
}