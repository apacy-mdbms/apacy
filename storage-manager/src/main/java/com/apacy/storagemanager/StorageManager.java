package com.apacy.storagemanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.Column;
import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.Statistic;
import com.apacy.common.enums.DataType;
import com.apacy.common.enums.IndexType;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.storagemanager.index.*;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.stream.Collectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;


public class StorageManager extends DBMSComponent implements IStorageManager {

    private final BlockManager blockManager;
    private final Serializer serializer;
    private final StatsCollector statsCollector;
    private final CatalogManager catalogManager;
    private final IndexManager indexManager; // Helper class untuk B+Tree/Hash

    public StorageManager(String dataDirectory) {
        super("Storage Manager");
        this.catalogManager = new CatalogManager(dataDirectory + "/system_catalog.dat");
        this.blockManager = new BlockManager(dataDirectory);
        this.serializer = new Serializer(this.catalogManager);
        this.statsCollector = new StatsCollector(this.catalogManager, this.blockManager, this.serializer);
        this.indexManager = new IndexManager();
        // ... inisialisasi komponen internal
    }

    @Override
    public void initialize() {
        try {
            this.catalogManager.loadCatalog();

            for (Schema schema : catalogManager.getAllSchemas()) {
                for (IndexSchema idx : schema.indexes()) {
                    IIndex<?,?> index = createIndexInstance(schema, idx);
                    indexManager.register(schema.tableName(), idx.columnName(), idx.indexType().toString(), index);
                }
            }
            this.indexManager.loadAll(this.catalogManager);
        } catch (Exception e) {
            System.err.println("Gagal menginitialize Storage Manager! " + e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        // TODO: Implement storage manager shutdown logic
        // (e.g., flush buffers, close files)
        indexManager.flushAll(this.catalogManager); 
            // blockManager.flush();
        // } catch (IOException e) {
        //     System.err.println("Error flushing blocks during shutdown: " + e.getMessage());

    }

    public CatalogManager getCatalogManager() {
        return this.catalogManager;
    }

    private Object extractEqualityFilter(DataRetrieval req, Column colDef) {
        if (req.filterCondition() == null) return null;

        String cond = req.filterCondition().toString().replace(" ", "");
        String prefix = colDef.name() + "=";

        if (!cond.startsWith(prefix)) return null;

        String raw = cond.substring(prefix.length());

        return switch (colDef.type()) {
            case INTEGER -> Integer.parseInt(raw);
            case FLOAT   -> Float.parseFloat(raw);
            case CHAR    -> raw.charAt(0);
            case VARCHAR -> raw;
        };
    }


    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        try {
            Schema schema = catalogManager.getSchema(dataRetrieval.tableName());
            if (schema == null) {
                System.out.println(catalogManager.getAllSchemas());
                throw new IOException("Tabel tidak ditemukan di katalog: " + dataRetrieval.tableName());
            }
            String fileName = schema.dataFile(); 
            List<Row> allRows = new ArrayList<>();
            
            if (dataRetrieval.useIndex()) {
                for (IndexSchema idxSchema : schema.indexes()) {

                    IIndex index = indexManager.get(
                        schema.tableName(),
                        idxSchema.columnName(),
                        idxSchema.indexType().toString()
                    );

                    if (index == null) continue;
                    Object filterValue = extractEqualityFilter(dataRetrieval, schema.getColumnByName(idxSchema.columnName())); 
                    if (filterValue == null) continue;
                    List<Integer> ridList = index.getAddress(filterValue);
                    List<Row> resultRows = new ArrayList<>();
                    for (int encodedRid : ridList) {
                        long blockNo = (encodedRid >>> 16);
                        int slotNo = encodedRid & 0xFFFF; 

                        byte[] block = blockManager.readBlock(schema.dataFile(), blockNo);
                        Row r = serializer.readRowAtSlot(block, schema, slotNo);
                        if (r != null) resultRows.add(r);
                    }
                    return resultRows;
                }
            }


            // --- Alur Full Table Scan ---
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
            String fileName = schema.dataFile();

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
            int newSlotId;
            try {
                byte[] updatedBlockData = serializer.packRowToBlock(blockData, dataWrite.newData(), schema);
                blockManager.writeBlock(fileName, targetBlockNumber, updatedBlockData);
                newSlotId = serializer.getLastPackedSlotId();  

            } catch (IOException ex) { // Blok terakhir penuh
                byte[] newBlockData = serializer.initializeNewBlock();
                
                newBlockData = serializer.packRowToBlock(newBlockData, dataWrite.newData(), schema);
                
                targetBlockNumber = blockManager.appendBlock(fileName, newBlockData); 
                newSlotId = serializer.getLastPackedSlotId(); 
            }
            
            blockManager.flush();
            
            for (IndexSchema idxSchema : schema.indexes()) {

                IIndex index = indexManager.get(
                    schema.tableName(),
                    idxSchema.columnName(),
                    idxSchema.indexType().toString()
                );

                if (index != null) {
                    Object key = dataWrite.newData().data().get(idxSchema.columnName());

                    int ridValue = (int) ((targetBlockNumber << 16) | (newSlotId & 0xFFFF));
                    index.insertData(key, ridValue);
                    index.writeToFile(this.catalogManager);
                }
            }


            return 1; 

        } catch (IOException e) {
            System.err.println("Error writing block: " + e.getMessage());
            return 0;
        }
        // throw new UnsupportedOperationException("writeBlock not implemented yet");
    }

    public void createTable(Schema newSchema) throws IOException {
        System.out.println("StorageManager: Menerima perintah CREATE TABLE untuk: " + newSchema.tableName());
        // 1. Tambahkan skema baru ke cache memori Katalog
        catalogManager.addSchemaToCache(newSchema);
        
        // 2. Tulis ulang (flush) seluruh katalog ke disk
        catalogManager.writeCatalog();
        
        // 3. Buat file .dat kosong (dengan 1 blok header)
        byte[] initialBlock = serializer.initializeNewBlock();
        blockManager.writeBlock(newSchema.dataFile(), 0, initialBlock);
        
        // 4. TODO: Buat file .idx (jika ada indeks)
        for (IndexSchema idxSchema : newSchema.indexes()) {
            IIndex<?, ?> index = createIndexInstance(newSchema, idxSchema);
            indexManager.register(newSchema.tableName(), idxSchema.columnName(), idxSchema.indexType().toString(), index);
            index.writeToFile(this.catalogManager); // Tulis file .idx kosong
        }
        
        System.out.println("StorageManager: Tabel " + newSchema.tableName() + " berhasil dibuat di disk.");
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
        return this.statsCollector.getAllStats();
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

    private IIndex<?, ?> createIndexInstance(Schema tableSchema, IndexSchema idxSchema) throws IOException {
        Column col = tableSchema.getColumnByName(idxSchema.columnName());
        if (col == null) {
            throw new IOException("Kolom '" + idxSchema.columnName() + "' untuk indeks tidak ditemukan.");
        }
        
        DataType keyType = col.type();
        DataType valueType = DataType.INTEGER; 

        if (idxSchema.indexType() == IndexType.Hash) {
            return new HashIndex<>(
                tableSchema.tableName(),
                col.name(),
                idxSchema.indexFile(),
                keyType,
                valueType, // Tipe V (Value) -> Asumsi kita simpan Integer (RID)
                this.blockManager,
                this.serializer
            );
        } else if (idxSchema.indexType() == IndexType.BPlusTree) {
            // return new BPlusIndex<>(...); // (Implementasi BPlusIndex masih stub)
            throw new UnsupportedOperationException("BPlusTree index belum didukung.");
        } else {
            throw new UnsupportedOperationException("Tipe indeks tidak dikenal: " + idxSchema.indexType());
        }
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

    public BlockManager getBlockManager() {
        return this.blockManager;
    }

    public static void main(String[] args) {
        
        // --- 1. Inisialisasi ---
        System.out.println("--- 1. Inisialisasi StorageManager ---");
        String dataDir = "../data"; 
        StorageManager sm = new StorageManager(dataDir);
        try {
            sm.initialize();
            System.out.println("Storage Manager berhasil diinisialisasi.");
        } catch (Exception e) {
            System.err.println("Gagal inisialisasi:");
            e.printStackTrace();
            return; // Keluar jika inisialisasi gagal
        }
        
        System.out.println("\n--- 2. Membuat Tabel Baru 'dosen' ---");
        try {
            // Definisikan skema untuk tabel baru
            List<Column> dosenColumns = List.of(
                new Column("nidn", DataType.VARCHAR, 10),
                new Column("nama_dosen", DataType.VARCHAR, 100),
                new Column("email", DataType.VARCHAR, 100)
            );
            
            // Definisikan indeks untuk tabel baru
            List<IndexSchema> dosenIndexes = List.of(
                new IndexSchema("idx_dosen_nidn", "nidn", IndexType.Hash, "dosen_nidn.idx")
            );
            
            Schema dosenSchema = new Schema(
                "dosen",
                "dosen_table.dat", // Nama file datanya
                dosenColumns,
                dosenIndexes
            );
            
            // Panggil metode createTable
            sm.createTable(dosenSchema);
            
        } catch (IOException e) {
            System.err.println("Gagal membuat tabel 'dosen': " + e.getMessage());
        }
        
        System.out.println("\n--- 3. Mengisi (INSERT) 3 Nilai ke 'dosen' ---");
        try {
            Row row1 = new Row(Map.of("nidn", "12345", "nama_dosen", "Dr. Budi", "email", "budi@if.itb.ac.id"));
            Row row2 = new Row(Map.of("nidn", "67890", "nama_dosen", "Dr. Ani", "email", "ani@if.itb.ac.id"));
            Row row3 = new Row(Map.of("nidn", "10101", "nama_dosen", "Dr. Candra", "email", "candra@if.itb.ac.id"));
            
            DataWrite write1 = new DataWrite("dosen", row1, null);
            DataWrite write2 = new DataWrite("dosen", row2, null);
            DataWrite write3 = new DataWrite("dosen", row3, null);
            
            sm.writeBlock(write1);
            sm.writeBlock(write2);
            sm.writeBlock(write3);
            
            System.out.println("3 baris berhasil di-INSERT ke tabel 'dosen'.");
            
        } catch (Exception e) {
            System.err.println("Gagal INSERT ke tabel 'dosen': " + e.getMessage());
        }

        System.out.println("\n--- 4. Membaca (READ) 3 Nilai dari 'dosen' ---");
        try {
            // Buat DTO DataRetrieval untuk SELECT * FROM dosen
            DataRetrieval readReq = new DataRetrieval(
                "dosen",          // tableName
                List.of("*"),     // columns ("*")
                null,             // filters (null)
                false              // indexName (null = Full Scan)
            );
            
            List<Row> results = sm.readBlock(readReq);
            
            System.out.println("Hasil Full Table Scan 'dosen' (ditemukan " + results.size() + " baris):");
            for (Row row : results) {
                System.out.println("  -> " + row.data());
            }

            // Uji Proyeksi (hanya NIDN dan Email)
            System.out.println("\n--- 5. Membaca (READ) dengan Proyeksi ---");
            DataRetrieval readReqProject = new DataRetrieval(
                "dosen",
                List.of("nidn","email"), // Hanya minta 2 kolom
                null,
                false
            );
            
            List<Row> projectedResults = sm.readBlock(readReqProject);
            System.out.println("Hasil Proyeksi 'dosen' (ditemukan " + projectedResults.size() + " baris):");
            for (Row row : projectedResults) {
                System.out.println("  -> " + row.data());
            }

        } catch (Exception e) {
            System.err.println("Gagal READ dari tabel 'dosen': " + e.getMessage());
        }
        
        System.out.println("\n--- 6. Shutdown ---");
        sm.shutdown();
    }
}
