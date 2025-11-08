package com.apacy.storagemanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IStorageManager;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.HashMap;


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
        try {
            String fileName = getFileNameForTable(dataRetrieval.tableName());
            List<Row> allRows = new ArrayList<>();
            
            long blockCount = blockManager.getBlockCount(fileName);

            for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                byte[] blockData = blockManager.readBlock(fileName, blockNumber);
                List<Row> rowsOfBlock = serializer.deserializeBlock(blockData);
                allRows.addAll(rowsOfBlock);
            }

            return allRows.stream()
                .filter(row -> applyFilter(row, dataRetrieval))
                .collect(Collectors.toList());

        } catch (IOException e) {
            System.err.println("Error reading block: " + e.getMessage());
            return Collections.emptyList();
        }
        // throw new UnsupportedOperationException("readBlock not implemented yet");
    }

    @Override
    public int writeBlock(DataWrite dataWrite) {
        // TODO:
        // 1. Panggil serializer.serialize(dataWrite.newData())
        // 2. Panggil blockManager.writeBlock(...)
        // 3. Update index
        // 4. Kembalikan jumlah baris
        try {
            String fileName = getFileNameForTable(dataWrite.tableName());

            long blockCount = blockManager.getBlockCount(fileName);
            byte[] blockData;
            long targetBlockNumber;

            if (blockCount == 0) {
                // buat blok baru
                blockData = serializer.initializeNewBlock();
                targetBlockNumber = 0;
            } else {
                // ambil blok terakhir
                targetBlockNumber = blockCount - 1;
                blockData = blockManager.readBlock(fileName, targetBlockNumber);
            }

            try {
                blockData = serializer.packRowToBlock(blockData, dataWrite.newData());
                blockManager.writeBlock(fileName, targetBlockNumber, blockData);
                blockManager.flush();
                // TODO: index update kondisional
                // TODO: stats update
                return 1;
            } catch (IOException ex) {
                if (blockCount > 0) {
                    blockData = serializer.initializeNewBlock();
                    blockData = serializer.packRowToBlock(blockData, dataWrite.newData());
                    long newBlockNumber = blockManager.appendBlock(fileName, blockData);
                    // TODO: update index dengan dengan newblock
                    // TODO: update stats dengan newblock
                    System.out.println("Appended new block number: " + newBlockNumber);
                    blockManager.flush();
                    return 1;
                }
                throw ex;
            }
        } catch (IOException e) {
            System.err.println("Error writing block: " + e.getMessage());
            return 0;
        }
        // throw new UnsupportedOperationException("writeBlock not implemented yet");
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        // TODO:
        // 1. Cari blok yang mau dihapus
        // 2. Hapus/modifikasi bloknya
        // 3. Update index
        // 4. Kembalikan jumlah baris
        try {
            String fileName = getFileNameForTable(dataDeletion.tableName());
            int deletedCount = 0;
            List<Row> deletedRows = new ArrayList<>();

            long blockCount = blockManager.getBlockCount(fileName);

            for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                byte[] blockData = blockManager.readBlock(fileName, blockNumber);
                List<Row> rowsOfBlock = serializer.deserializeBlock(blockData);

                List<Row> remainingRows = new ArrayList<>();
                for (Row row : rowsOfBlock) {
                    if (matchesDeleteCondition(row, dataDeletion)) {
                        deletedCount = deletedCount + 1;
                        deletedRows.add(row); // ini buat update index nanti
                    } else {
                        remainingRows.add(row);
                    }
                }

                if (!remainingRows.isEmpty()) {
                    byte[] newBlockData = serializer.initializeNewBlock();
                    for (Row row : remainingRows) {
                        newBlockData = serializer.packRowToBlock(newBlockData, row);
                    }
                    blockManager.writeBlock(fileName, blockNumber, newBlockData);
                } else if (rowsOfBlock.size() > 0) {
                    blockManager.writeBlock(fileName, blockNumber, serializer.initializeNewBlock());
                }
            }

            blockManager.flush();
            // TODO: update index
            // TODO: update stats
            // if (deletedCount > 0) {

            // }
            return deletedCount;
        } catch (IOException e) {
            System.err.println("Error deleting block: " + e.getMessage());
            return 0;
        }
        // throw new UnsupportedOperationException("deleteBlock not implemented yet");
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        // TODO:
        // 1. Panggil indexManager untuk membuat index baru
        // (misal: new HashIndex(table, column))
        throw new UnsupportedOperationException("setIndex not implemented yet");
    }

    @Override
    public Map<String, Statistic> getAllStats() {
        // TODO:
        // 1. Panggil statsCollector.collectStats()
        // 2. Kembalikan map dari String, Statistic (Nama tabel dan statistiknya)
        throw new UnsupportedOperationException("getStats not implemented yet");
    }

    /** 
     * Konversi nama tabel ke nama file
     * format ditentukannya "tableName_table.dat"
     * @param tableName as nama tablenya
     * @return fileName as nama filenya 
     */
    private String getFileNameForTable(String tableName) {
        return tableName + "_table.dat";
    }

    /**
     * Apply filter condition ke sebuah row
     * @param row as row yang mau difilter
     * @param dataRetrieval as data yang ingin dicek
     * @return true jika lolos filter
     */
    private boolean applyFilter(Row row, DataRetrieval dataRetrieval) {
        if (dataRetrieval.filterCondition() == null) {
            return true;
        }
        // TODO: parse sama evaluasi filterCondition
        // TODO: tergantung tipe/struktur nanti di query processing
        // masih dummy dan dianngap lolos selain null..
        return true;
    }

    private boolean matchesDeleteCondition(Row row, DataDeletion dataDeletion) {
        if (dataDeletion.filterCondition() == null) {
            return true;
        }
        // TODO: parse sama evaluasi filterCondition
        return false;
    }
}