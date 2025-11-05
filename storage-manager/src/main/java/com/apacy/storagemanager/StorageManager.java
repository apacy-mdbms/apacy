package com.apacy.storagemanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IStorageManager;
import java.util.List;

public class StorageManager extends DBMSComponent implements IStorageManager {

    private final BlockManager blockManager;
    private final Serializer serializer;
    private final StatsCollector statsCollector;
    // private final IndexManager indexManager; // Helper class untuk B+Tree/Hash

    public StorageManager(String dataDirectory) {
        super("Storage Manager");
        this.blockManager = new BlockManager(dataDirectory);
        this.serializer = new Serializer();
        this.statsCollector = new StatsCollector();
        // ... inisialisasi komponen internal
    }

    @Override
    public void initialize() throws Exception {
        // TODO: Implement storage manager initialization logic
        // (e.g., load metadata, open files)
    }

    @Override
    public void shutdown() {
        // TODO: Implement storage manager shutdown logic
        // (e.g., flush buffers, close files)
    }

    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        // TODO:
        // 1. Panggil blockManager.readBlock(...)
        // 2. Untuk tiap blok, panggil serializer.deserialize(...)
        // 3. Filter hasilnya sesuai dataRetrieval.filterCondition
        // 4. Kembalikan List<Row>
        throw new UnsupportedOperationException("readBlock not implemented yet");
    }

    @Override
    public int writeBlock(DataWrite dataWrite) {
        // TODO:
        // 1. Panggil serializer.serialize(dataWrite.newData())
        // 2. Panggil blockManager.writeBlock(...)
        // 3. Update index
        // 4. Kembalikan jumlah baris
        throw new UnsupportedOperationException("writeBlock not implemented yet");
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        // TODO:
        // 1. Cari blok yang mau dihapus
        // 2. Hapus/modifikasi bloknya
        // 3. Update index
        // 4. Kembalikan jumlah baris
        throw new UnsupportedOperationException("deleteBlock not implemented yet");
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        // TODO:
        // 1. Panggil indexManager untuk membuat index baru
        // (misal: new HashIndex(table, column))
        throw new UnsupportedOperationException("setIndex not implemented yet");
    }

    @Override
    public Statistic getStats() {
        // TODO:
        // 1. Panggil statsCollector.collectStats()
        // 2. Kembalikan objek Statistic
        throw new UnsupportedOperationException("getStats not implemented yet");
    }
}