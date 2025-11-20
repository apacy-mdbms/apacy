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
                    IIndex<?, ?> index = createIndexInstance(schema, idx);
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

        indexManager.flushAll(this.catalogManager);

    }

    public CatalogManager getCatalogManager() {
        return this.catalogManager;
    }

    private Object extractEqualityFilter(Map<String, Object> parsedFilters, Column colDef) {
        if (parsedFilters == null || colDef == null) {
            return null;
        }
        return parsedFilters.get(colDef.name());
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
            Map<String, Object> parsedFilter = parseFilterCondition(dataRetrieval.filterCondition(), schema);

            if (dataRetrieval.useIndex() && !parsedFilter.isEmpty()) {
                for (IndexSchema idxSchema : schema.indexes()) {
                    IIndex index = indexManager.get(
                            schema.tableName(),
                            idxSchema.columnName(),
                            idxSchema.indexType().toString());

                    if (index == null)
                        continue;
                    Object filterValue = parsedFilter.get(idxSchema.columnName());
                    if (filterValue == null)
                        continue;
                    List<Integer> ridList = index.getAddress(filterValue);
                    List<Row> resultRows = new ArrayList<>();
                    for (int encodedRid : ridList) {
                        long blockNo = (encodedRid >>> 16);
                        int slotNo = encodedRid & 0xFFFF;

                        byte[] block = blockManager.readBlock(schema.dataFile(), blockNo);
                        Row r = serializer.readRowAtSlot(block, schema, slotNo);
                        if (r != null && rowMatchesFilters(r, parsedFilter))
                            resultRows.add(projectColumns(r, dataRetrieval.columns()));
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
            long targetBlockNumber = -1;
            int newSlotId = -1;
            boolean packed = false;
            // byte[] blockData;

            for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                byte[] candidate = blockManager.readBlock(fileName, blockNumber);
                try {
                    byte[] updated = serializer.packRowToBlock(candidate, dataWrite.newData(), schema);
                    blockManager.writeBlock(fileName, blockNumber, updated);
                    targetBlockNumber = blockNumber;
                    newSlotId = serializer.getLastPackedSlotId();
                    packed = true;
                    break;
                } catch (IOException fullBlock) {
                    // next block bro
                    continue;
                }
            }

            if (!packed) {
                byte[] newBlock = serializer.initializeNewBlock();
                newBlock = serializer.packRowToBlock(newBlock, dataWrite.newData(), schema);
                targetBlockNumber = blockManager.appendBlock(fileName, newBlock);
                newSlotId = serializer.getLastPackedSlotId();
            }

            blockManager.flush();

            for (IndexSchema idxSchema : schema.indexes()) {
                IIndex index = indexManager.get(schema.tableName(), idxSchema.columnName(),
                        idxSchema.indexType().toString());

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
            indexManager.register(newSchema.tableName(), idxSchema.columnName(), idxSchema.indexType().toString(),
                    index);
            index.writeToFile(this.catalogManager); // Tulis file .idx kosong
        }

        System.out.println("StorageManager: Tabel " + newSchema.tableName() + " berhasil dibuat di disk.");
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        try {
            Schema schema = catalogManager.getSchema(dataDeletion.tableName());
            if (schema == null) {
                throw new IOException("Table " + dataDeletion.tableName() + " not found");
            }

            Map<String, Object> filters = parseFilterCondition(dataDeletion.filterCondition(), schema);
            if (filters.isEmpty()) {
                throw new IOException("Filter delete is invalid or empty");
            }

            String fileName = schema.dataFile();
            long blockCount = blockManager.getBlockCount(fileName);
            int deletedRows = 0;

            for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                byte[] blockData = blockManager.readBlock(fileName, blockNumber);
                int slotCount = serializer.getSlotCount(blockData);
                boolean blockDirty = false;

                for (int slotId = 0; slotId < slotCount; slotId++) {
                    Row row = serializer.readRowAtSlot(blockData, schema, slotId);
                    if (row == null || !rowMatchesFilters(row, filters)) {
                        continue;
                    }

                    if (serializer.deleteSlot(blockData, slotId)) {
                        deletedRows++;
                        blockDirty = true;
                        removeRowFromIndexes(schema, blockNumber, slotId, row);
                    }
                }

                if (blockDirty) {
                    blockManager.writeBlock(fileName, blockNumber, blockData);
                }
            }

            blockManager.flush();
            return deletedRows;
        } catch (IOException e) {
            System.err.println("Error missing block: " + e.getMessage());
            return 0;
        }
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        try {
            Schema schema = catalogManager.getSchema(table);
            if (schema == null) {
                throw new IOException("Table " + table + " not found");
            }

            Column targetColumn = schema.getColumnByName(column);
            if (targetColumn == null) {
                throw new IOException("Column " + column + " not found on table " + table);
            }

            IndexType typeEnum = findIndexType(indexType);
            String suffix = typeEnum.name().toLowerCase();
            String indexFile = table + "_" + column + "_" + suffix + ".idx";
            String indexName = "idx_" + table + "_" + column + "_" + suffix;

            IndexSchema newIndexSchema = new IndexSchema(indexName, column, typeEnum, indexFile);
            IIndex<?, ?> index = createIndexInstance(schema, newIndexSchema);
            indexManager.register(table, column, typeEnum.toString(), index);

            @SuppressWarnings("unchecked")
            IIndex<Object, Integer> typedIndex = (IIndex<Object, Integer>) index;
            populateIndexFromTable(schema, typedIndex, column);
            index.writeToFile(catalogManager);

            List<IndexSchema> updatedIndexes = new ArrayList<>(schema.indexes());
            updatedIndexes.add(newIndexSchema);
            Schema updatedSchema = new Schema(schema.tableName(), schema.dataFile(), schema.columns(), updatedIndexes);
            catalogManager.updateSchema(updatedSchema);
            catalogManager.writeCatalog();
        } catch (IOException e) {
            System.err.println("Failed to create a new index: " + e.getMessage());
        }
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
        Schema schema = catalogManager.getSchema(dataRetrieval.tableName());
        Map<String, Object> filters = parseFilterCondition(dataRetrieval.filterCondition(), schema);
        return rowMatchesFilters(row, filters);
    }

    private boolean matchesDeleteCondition(Row row, DataDeletion dataDeletion) {
        Schema schema = catalogManager.getSchema(dataDeletion.tableName());
        Map<String, Object> filters = parseFilterCondition(dataDeletion.filterCondition(), schema);
        return rowMatchesFilters(row, filters);
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
                    this.serializer);
        } else if (idxSchema.indexType() == IndexType.BPlusTree) {
            int order = 100;
            return new BPlusIndex<>(
                    tableSchema.tableName(),
                    col.name(),
                    order,
                    idxSchema.indexFile(),
                    this.blockManager,
                    this.serializer);
        } else {
            throw new UnsupportedOperationException("Tipe indeks tidak dikenal: " + idxSchema.indexType());
        }
    }

    private void removeRowFromIndexes(Schema schema, long blockNumber, int slotId, Row row) {
        int ridValue = (int) ((blockNumber << 16) | (slotId & 0xFFFF));
        for (IndexSchema idxSchema : schema.indexes()) {
            IIndex index = indexManager.get(
                    schema.tableName(),
                    idxSchema.columnName(),
                    idxSchema.indexType().toString());
            if (index != null) {
                Object key = row.data().get(idxSchema.columnName());
                index.deleteData(key, ridValue);
                index.writeToFile(this.catalogManager);
            }
        }
    }

    private void populateIndexFromTable(Schema schema, IIndex<Object, Integer> index, String column)
            throws IOException {
        long blockCount = blockManager.getBlockCount(schema.dataFile());
        for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
            byte[] blockData = blockManager.readBlock(schema.dataFile(), blockNumber);
            int slotCount = serializer.getSlotCount(blockData);
            for (int slotId = 0; slotId < slotCount; slotId++) {
                Row row = serializer.readRowAtSlot(blockData, schema, slotId);
                if (row == null)
                    continue;
                Object key = row.data().get(column);
                int ridValue = (int) ((blockNumber << 16) | (slotId & 0xFFFF));
                index.insertData(key, ridValue);
            }
        }
    }

    private Map<String, Object> parseFilterCondition(Object filterCondition, Schema schema) {
        if (filterCondition == null || schema == null) {
            return Collections.emptyMap();
        }
        String raw = filterCondition.toString().trim();
        if (raw.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> parsed = new HashMap<>();
        String[] expressions = raw.split("(?i)AND");
        for (String expression : expressions) {
            String[] parts = expression.split("=");
            if (parts.length != 2)
                continue;
            String columnName = parts[0].trim();
            Column column = schema.getColumnByName(columnName);
            if (column == null)
                continue;

            String literal = stripQuotes(parts[1].trim());
            Object typedValue = convertLiteral(column, literal);
            if (typedValue != null) {
                parsed.put(column.name(), typedValue);
            }
        }
        return parsed;
    }

    private Object convertLiteral(Column column, String literal) {
        try {
            return switch (column.type()) {
                case INTEGER -> Integer.parseInt(literal);
                case FLOAT -> Float.parseFloat(literal);
                case CHAR -> literal.charAt(0);
                case VARCHAR -> literal;
            };
        } catch (Exception e) {
            return null;
        }
    }

    private String stripQuotes(String literal) {
        if (literal.length() >= 2) {
            if ((literal.startsWith("\"") && literal.endsWith("\"")) ||
                    (literal.startsWith("'") && literal.endsWith("'"))) {
                return literal.substring(1, literal.length() - 1);
            }
        }
        return literal;
    }

    private boolean rowMatchesFilters(Row row, Map<String, Object> filters) {
        if (filters == null || filters.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            Object value = row.data().get(entry.getKey());
            if (value == null || !value.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private IndexType findIndexType(String indexType) {
        return Arrays.stream(IndexType.values())
                .filter(t -> t.name().equalsIgnoreCase(indexType))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Tipe indeks tidak dikenal: " + indexType));
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
                    new Column("email", DataType.VARCHAR, 100));

            // Definisikan indeks untuk tabel baru
            List<IndexSchema> dosenIndexes = List.of(
                    new IndexSchema("idx_dosen_nidn", "nidn", IndexType.Hash, "dosen_nidn.idx"));

            Schema dosenSchema = new Schema(
                    "dosen",
                    "dosen_table.dat", // Nama file datanya
                    dosenColumns,
                    dosenIndexes);

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
                    "dosen", // tableName
                    List.of("*"), // columns ("*")
                    null, // filters (null)
                    false // indexName (null = Full Scan)
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
                    List.of("nidn", "email"), // Hanya minta 2 kolom
                    null,
                    false);

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
