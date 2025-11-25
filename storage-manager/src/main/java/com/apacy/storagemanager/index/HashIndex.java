package com.apacy.storagemanager.index;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.enums.DataType;
import com.apacy.storagemanager.BlockManager;
import com.apacy.storagemanager.CatalogManager;
import com.apacy.storagemanager.Serializer;

import java.io.IOException;
import java.util.*;

public class HashIndex<K, V> implements IIndex<K, V> {

    private final String tableName;
    private final String columnName;
    private final String indexFile;
    private final DataType keyType;
    private final DataType valueType;
    private final BlockManager blockManager;
    private final Serializer serializer;

    private HashTable<K,V> table;

    public HashIndex(String tableName,
                     String columnName,
                     String indexFile,
                     DataType keyType,
                     DataType valueType,
                     BlockManager blockManager,
                     Serializer serializer) {

        this.tableName = tableName;
        this.columnName = columnName;
        this.indexFile = indexFile;
        this.keyType = keyType;
        this.valueType = valueType;
        this.blockManager = blockManager;
        this.serializer = serializer;

        this.table = new HashTable<>(8, 4);
    }

    public Schema buildSchema(CatalogManager catalogManager) {
        Schema baseSchema = catalogManager.getSchema(tableName);
        if (baseSchema == null)
            throw new IllegalStateException("Base table '" + tableName + "' tidak ditemukan di CatalogManager.");

        Column baseColumn = baseSchema.getColumnByName(columnName);
        if (baseColumn == null)
            throw new IllegalStateException("Kolom '" + columnName + "' tidak ditemukan di tabel " + tableName);

        List<Column> indexColumns = List.of(
            new Column("key", baseColumn.type(), baseColumn.length()),
            new Column("value", DataType.INTEGER),
            new Column("nextBucket", DataType.INTEGER),
            new Column("startBucket", DataType.INTEGER),
            new Column("bucketCapacity", DataType.INTEGER)
        );

        return new Schema(
            tableName + "_" + columnName + "_hashindex",
            indexFile,
            indexColumns,
            List.of()
        );
    }

    @Override
    public void writeToFile(CatalogManager catalogManager) {
        try {
            Schema schema = buildSchema(catalogManager);
            List<Row> rows = new ArrayList<>();

            for (Bucket<K,V> bucket : table.getBuckets()) {
                for (Bucket.Entry<K,V> entry : bucket.getEntries()) {
                    Map<String,Object> data = new HashMap<>();

                    data.put("key", entry.key);
                    data.put("value", entry.value);
                    data.put("nextBucket", bucket.getNextBucket());
                    data.put("startBucket", bucket.startBucket ? 1 : 0);
                    data.put("bucketCapacity", bucket.getCapacity());

                    rows.add(new Row(data));
                }
            }

            byte[] block = serializer.initializeNewBlock();
            long blockNo = 0;

            for (Row r : rows) {
                try {
                    block = serializer.packRowToBlock(block, r, schema);
                } catch (IOException full) {
                    blockManager.writeBlock(indexFile, blockNo, block);
                    block = serializer.initializeNewBlock();
                    block = serializer.packRowToBlock(block, r, schema);
                    blockNo = blockManager.appendBlock(indexFile, block);
                }
            }

            blockManager.writeBlock(indexFile, blockNo, block);
            blockManager.flush();

        } catch (Exception e) {
            System.err.println("HashIndex.writeToFile error: " + e.getMessage());
        }
    }

    @Override
    public void loadFromFile(CatalogManager catalogManager) {
        try {
            Schema schema = buildSchema(catalogManager);
            long blockCount = blockManager.getBlockCount(indexFile);
            HashTable<K,V> newTable = new HashTable<>(8, 4); // fresh table

            for (long bn = 0; bn < blockCount; bn++) {
                byte[] blk = blockManager.readBlock(indexFile, bn);
                List<Row> rows = serializer.deserializeBlock(blk, schema);

                for (Row r : rows) {
                    K key = (K) r.data().get("key");
                    V value = (V) r.data().get("value");
                    int next = (int) r.data().get("nextBucket");
                    int start = (int) r.data().get("startBucket");
                    int cap = (int) r.data().get("bucketCapacity");

                    newTable.insert(key, value);
                }
            }

            this.table = newTable;

        } catch (Exception e) {
            System.err.println("HashIndex.loadFromFile error: " + e.getMessage());
        }
    }

    @Override
    public void insertData(K key, V address) {
        table.insert(key, address);
    }

    @Override
    public void deleteData(K key, V address) {
        table.remove(key);
    }

    @Override
    public List<V> getAddress(K key) {
        return table.get(key);
    }

    @Override
    public void remove() {
        // wipe index file
        try {
            byte[] b = serializer.initializeNewBlock();
            blockManager.writeBlock(indexFile, 0, b);
            blockManager.flush();
        } catch (IOException ex) {
            System.err.println("Cannot reset index file: " + ex.getMessage());
        }
    }
}
