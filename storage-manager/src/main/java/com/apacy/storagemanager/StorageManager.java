package com.apacy.storagemanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Statistic;
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
    private final CatalogManager catalogManager;
    // private final IndexManager indexManager; // Helper class untuk B+Tree/Hash

    public StorageManager(String dataDirectory) {
        super("Storage Manager");
        this.catalogManager = new CatalogManager(dataDirectory + "/system_catalog.dat");
        this.blockManager = new BlockManager(dataDirectory);
        this.serializer = new Serializer();
        this.statsCollector = new StatsCollector();
        // ... inisialisasi komponen internal
    }

    @Override
    public void initialize() throws Exception {
        try {
            this.catalogManager.loadCatalog();
            // TODO: Inisialisasi/Rebuild indeks
            // this.indexManager.initialize();
        } catch (IOException e) {
            System.err.println("FATAL: Gagal memuat System Catalog! " + e.getMessage());
            throw new Exception("Gagal memuat System Catalog!", e);
        }
    }

    @Override
    public void shutdown() {
        // TODO: Implement storage manager shutdown logic
        // (e.g., flush buffers, close files)
        try {
            blockManager.flush();
        } catch (IOException e) {
            System.err.println("Error flushing blocks during shutdown: " + e.getMessage());
        }
    }

    public CatalogManager getCatalogManager() {
        return this.catalogManager;
    }

    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        // TODO:
        // 1. Panggil blockManager.readBlock(...)
        // 2. Untuk tiap blok, panggil serializer.deserialize(...)
        // 3. Filter hasilnya sesuai dataRetrieval.filterCondition
        // 4. Kembalikan List<Row>
        try {
            Schema schema = catalogManager.getSchema(dataRetrieval.tableName());
            if (schema == null) {
                throw new IOException("Tabel tidak ditemukan di katalog: " + dataRetrieval.tableName());
            }

            String fileName = getFileNameForTable(dataRetrieval.tableName());
            List<Row> allRows = new ArrayList<>();
            
            long blockCount = blockManager.getBlockCount(fileName);

            for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                byte[] blockData = blockManager.readBlock(fileName, blockNumber);
                List<Row> rowsOfBlock = serializer.deserializeBlock(blockData, schema);
                allRows.addAll(rowsOfBlock);
            }
            List<String> requestedColumns = dataRetrieval.columns();

            return allRows.stream()
                .filter((Row row) -> applyFilter(row, dataRetrieval))
                .map((Row row) -> projectColumns(row, requestedColumns))
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
            Schema schema = catalogManager.getSchema(dataWrite.tableName());
            if (schema == null) {
                throw new IOException("Tabel tidak ditemukan: " + dataWrite.tableName());
            }
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
                blockData = serializer.packRowToBlock(blockData, dataWrite.newData(), schema);
                blockManager.writeBlock(fileName, targetBlockNumber, blockData);
                blockManager.flush();
                // TODO: index update kondisional
                // TODO: stats update
                return 1;

            } catch (IOException ex) {
                if (blockCount > 0) {
                    blockData = serializer.initializeNewBlock();
                    blockData = serializer.packRowToBlock(blockData, dataWrite.newData(), schema);
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
        // PERINGATAN: Logika "re-write" ini akan merusak indeks.
        // Anda HARUS beralih ke logika "menandai slot sebagai terhapus".
        
        // TODO: Implementasi delete yang benar (menggunakan serializer.deleteSlot)
        throw new UnsupportedOperationException("DeleteBlock (rewrite) tidak aman untuk indeks. Ganti ke delete by slot.");
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

    private Row projectColumns(Row fullRow, List<String> requestedColumns) {
        if (requestedColumns == null || requestedColumns.contains("*")) {
            return fullRow; // Kembalikan semua jika minta "*"
        }

        Map<String, Object> projectedData = new HashMap<>();
        for (String colName : requestedColumns) {
            if (fullRow.data().containsKey(colName)) {
                projectedData.put(colName, fullRow.data().get(colName));
            }
        }
        return new Row(projectedData);
    }
}