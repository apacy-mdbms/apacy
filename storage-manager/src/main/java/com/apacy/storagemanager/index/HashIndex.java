package com.apacy.storagemanager.index;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.enums.DataType;
import com.apacy.storagemanager.BlockManager;
import com.apacy.storagemanager.Serializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * HashIndex menyimpan pasangan (key, address) ke dalam HashTable<K, List<V>>
 * dan melakukan persistensi ke file .idx menggunakan BlockManager + Serializer
 * dengan format "slotted page" yang sama dengan tabel biasa.
 */
public class HashIndex<K, V> implements IIndex<K, V> {

    private final String tableName;
    private final String columnName;
    private final String indexFileName;
    private final BlockManager blockManager;
    private final Serializer serializer;

    private final DataType keyType;
    private final DataType valueType;
    private final Schema indexSchema;

    // Struktur in-memory
    private HashTable<K, List<V>> table;
    private final List<IndexEntry<K, V>> records = new ArrayList<>();

    // Entry sederhana untuk serialisasi
    private static final class IndexEntry<K, V> {
        final K key;
        final V value;

        IndexEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * @param tableName     nama tabel yang di-index (misal "dosen")
     * @param columnName    nama kolom yang di-index (misal "nidn")
     * @param indexFileName nama file index (misal "dosen_nidn.idx")
     * @param keyType       DataType dari kolom yang di-index
     * @param valueType     DataType untuk "alamat" yang disimpan (V)
     * @param blockManager  BlockManager yang sama dengan StorageManager
     * @param serializer    Serializer yang sama dengan StorageManager
     */
    public HashIndex(
            String tableName,
            String columnName,
            String indexFileName,
            DataType keyType,
            DataType valueType,
            BlockManager blockManager,
            Serializer serializer
    ) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexFileName = indexFileName;
        this.blockManager = blockManager;
        this.serializer = serializer;
        this.keyType = keyType;
        this.valueType = valueType;

        this.table = new HashTable<>();
        this.indexSchema = buildIndexSchema();
    }

    // ---------- Helper schema & path ----------

    private Schema buildIndexSchema() {
        List<Column> cols = new ArrayList<>();
        cols.add(new Column("key", keyType, defaultLengthFor(keyType)));
        cols.add(new Column("value", valueType, defaultLengthFor(valueType)));

        // Index ini sendiri tidak memiliki index lagi, jadi List<IndexSchema> kosong.
        return new Schema(
                tableName + "_" + columnName + "_hash_index",
                indexFileName,
                cols,
                Collections.emptyList()
        );
    }

    private int defaultLengthFor(DataType type) {
        // Bebas, ini cuma untuk CHAR/VARCHAR; INTEGER/FLOAT boleh 0
        return switch (type) {
            case CHAR -> 1;
            case VARCHAR -> 255;
            default -> 0;
        };
    }

    private Path getIndexPath() {
        return Paths.get(blockManager.getDataDirectory(), indexFileName);
    }

    private Row entryToRow(IndexEntry<K, V> e) {
        Map<String, Object> data = new HashMap<>();
        data.put("key", e.key);
        data.put("value", e.value);
        return new Row(data);
    }

    private void insertInternal(K key, V address, boolean addToRecords) {
        List<V> list = table.get(key);
        if (list == null) {
            list = new ArrayList<>();
            table.insert(key, list);
        }
        list.add(address);

        if (addToRecords) {
            records.add(new IndexEntry<>(key, address));
        }
    }

    // ---------- Implementasi IIndex ----------

    /**
     * Hapus file index (.idx) dari disk dan kosongkan struktur in-memory.
     */
    @Override
    public void remove() {
        try {
            Files.deleteIfExists(getIndexPath());
        } catch (IOException e) {
            System.err.println("[HashIndex] Gagal menghapus file index " +
                    indexFileName + ": " + e.getMessage());
        }
        this.table = new HashTable<>();
        this.records.clear();
    }

    /**
     * Load seluruh isi index dari file .idx ke memori:
     * baca blok 4KB, deserialisasi Row (key, value), lalu isi HashTable.
     */
    @Override
    public void loadFromFile() {
        this.table = new HashTable<>();
        this.records.clear();

        Path path = getIndexPath();

        try {
            // Kalau file belum ada → buat file index kosong (1 blok header)
            if (!Files.exists(path) || Files.size(path) == 0) {
                byte[] block = serializer.initializeNewBlock();
                blockManager.writeBlock(indexFileName, 0, block);
                blockManager.flush();
                return;
            }

            long blockCount = blockManager.getBlockCount(indexFileName);
            for (long b = 0; b < blockCount; b++) {
                byte[] blockData = blockManager.readBlock(indexFileName, b);
                List<Row> rows = serializer.deserializeBlock(blockData, indexSchema);

                for (Row row : rows) {
                    @SuppressWarnings("unchecked")
                    K key = (K) row.data().get("key");
                    @SuppressWarnings("unchecked")
                    V value = (V) row.data().get("value");
                    // Saat load dari file, kita isi HashTable dan records
                    insertInternal(key, value, true);
                }
            }
        } catch (IOException e) {
            System.err.println("[HashIndex] Gagal loadFromFile untuk " +
                    indexFileName + ": " + e.getMessage());
        }
    }

    /**
     * Tulis ulang isi index ke file .idx:
     *   - hapus file lama
     *   - buat blok baru
     *   - pack Row (key, value) ke blok pakai Serializer (slotted page)
     */
    @Override
    public void writeToFile() {
        Path path = getIndexPath();

        try {
            // Supaya gampang, kita re-write full file dari nol
            Files.deleteIfExists(path);

            if (records.isEmpty()) {
                // Index kosong → simpan 1 blok header saja
                byte[] empty = serializer.initializeNewBlock();
                blockManager.writeBlock(indexFileName, 0, empty);
                blockManager.flush();
                return;
            }

            long blockNumber = 0;
            byte[] currentBlock = serializer.initializeNewBlock();
            boolean blockHasData = false;

            for (IndexEntry<K, V> e : records) {
                Row row = entryToRow(e);
                try {
                    currentBlock = serializer.packRowToBlock(currentBlock, row, indexSchema);
                    blockHasData = true;
                } catch (IOException full) {
                    // Blok penuh → tulis blok lama, mulai blok baru
                    blockManager.writeBlock(indexFileName, blockNumber, currentBlock);
                    blockNumber++;

                    currentBlock = serializer.initializeNewBlock();
                    currentBlock = serializer.packRowToBlock(currentBlock, row, indexSchema);
                    blockHasData = true;
                }
            }

            if (blockHasData) {
                blockManager.writeBlock(indexFileName, blockNumber, currentBlock);
            }

            blockManager.flush();
        } catch (IOException e) {
            System.err.println("[HashIndex] Gagal writeToFile untuk " +
                    indexFileName + ": " + e.getMessage());
        }
    }

    /**
     * Ambil semua address untuk key tertentu.
     * Karena di dalam HashTable kita simpan List<V>, di sini kita balikin copy-nya.
     */
    @Override
    public List<V> getAddress(K key) {
        List<V> list = table.get(key);
        if (list == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(list);
    }

    /**
     * Insert data baru ke memori dulu (HashTable + records).
     * Persist-nya dilakukan saat writeToFile() dipanggil.
     */
    @Override
    public void insertData(K key, V address) {
        insertInternal(key, address, true);
    }

    /**
     * Hapus pasangan (key, address) dari struktur in-memory.
     * File .idx akan disinkronkan dengan writeToFile().
     */
    @Override
    public void deleteData(K key, V address) {
        List<V> list = table.get(key);
        if (list == null) return;

        boolean removedFromList = list.remove(address);
        if (!removedFromList) return;

        if (list.isEmpty()) {
            table.remove(key);
        }

        // Hapus satu entry yang cocok dari records
        for (int i = records.size() - 1; i >= 0; i--) {
            IndexEntry<K, V> e = records.get(i);
            if (Objects.equals(e.key, key) && Objects.equals(e.value, address)) {
                records.remove(i);
                break;
            }
        }
    }
}
