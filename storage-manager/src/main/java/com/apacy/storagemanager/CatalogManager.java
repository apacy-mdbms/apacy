package com.apacy.storagemanager;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Schema;
import com.apacy.common.enums.*;

/**
 * Mengelola metadata database (skema tabel, kolom, dan indeks).
 * Membaca dan menulis ke file biner 'system_catalog.dat'.
 * Disesuaikan untuk bekerja dengan kelas Schema dan Column yang ada.
 */
public class CatalogManager {

    private final String catalogFilePath;
    private final int MAGIC_NUMBER = 0xACDB01; // Penanda file katalog kita

    // Cache di memori, sekarang menggunakan kelas Schema Anda
    private Map<String, Schema> schemaCache;

    // Cache terpisah untuk data yang tidak disimpan di kelas Schema Anda
    private Map<String, String> dataFileCache;
    // private Map<String, List<IndexSchema>> indexCache;

    public CatalogManager(String catalogFilePath) {
        this.catalogFilePath = catalogFilePath;
        this.schemaCache = new HashMap<>();
        this.dataFileCache = new HashMap<>();
        // this.indexCache = new HashMap<>();
    }

    /**
     * Dipanggil oleh StorageManager.initialize() saat startup.
     * Membaca file 'system_catalog.dat' dan memuatnya ke cache memori.
     */
    public void loadCatalog() throws IOException {
        System.out.println("CatalogManager: Memuat katalog dari " + catalogFilePath + "...");
        
        // Kosongkan cache lama
        this.schemaCache.clear();
        this.dataFileCache.clear();
        // this.indexCache.clear();

        try (DataInputStream dis = new DataInputStream(new FileInputStream(catalogFilePath))) {
            
            int magic = dis.readInt();
            if (magic != MAGIC_NUMBER) {
                throw new IOException("Bukan file system_catalog.dat yang valid.");
            }
            int tableCount = dis.readInt();
            System.out.println("CatalogManager: Menemukan " + tableCount + " tabel.");

            for (int i = 0; i < tableCount; i++) {
                
                String tableName = dis.readUTF();
                String dataFile = dis.readUTF();
                int columnCount = dis.readInt();

                // Buat List<Column> (menggunakan kelas Column Anda)
                List<Column> columns = new ArrayList<>();
                for (int j = 0; j < columnCount; j++) {
                    String colName = dis.readUTF();
                    int colTypeInt = dis.readInt();
                    int colLength = dis.readInt();
                    
                    // Konversi int dari file ke enum DataType Anda
                    DataType colType = DataType.fromValue(colTypeInt); 
                    
                    // Buat objek Column Anda
                    columns.add(new Column(colName, colType, colLength));
                }
                
                // Baca Indeks
                int indexCount = dis.readInt();
                List<IndexSchema> indexes = new ArrayList<>();
                for (int k = 0; k < indexCount; k++) {
                    String idxName = dis.readUTF();
                    String idxColName = dis.readUTF();
                    int idxTypeInt = dis.readInt(); 
                    IndexType idxType = IndexType.fromValue(idxTypeInt);
                    String idxFile = dis.readUTF();
                    indexes.add(new IndexSchema(idxName, idxColName, idxType, idxFile));
                }
                
                // Buat record Schema "all-in-one"
                Schema schema = new Schema(tableName, dataFile, columns, indexes);
                // Simpan semuanya ke cache memori
                this.schemaCache.put(tableName, schema);
                this.dataFileCache.put(tableName, dataFile);
                // this.indexCache.put(tableName, indexes);
                System.out.println("CatalogManager: Memuat skema untuk tabel '" + tableName + "'.");
            }
        }
        System.out.println("CatalogManager: Katalog berhasil dimuat ke memori.");
    }

    /**
     * (Untuk Bonus) Menulis ulang seluruh cache memori ke file 'system_catalog.dat'.
     */
    public void writeCatalog() throws IOException {
        System.out.println("CatalogManager: Menulis ulang katalog ke disk...");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(catalogFilePath))) {
            
            dos.writeInt(MAGIC_NUMBER);
            dos.writeInt(schemaCache.size()); 

            for (String tableName : schemaCache.keySet()) {
                // Ambil semua data dari cache
                Schema schema = schemaCache.get(tableName);
                String dataFile = dataFileCache.get(tableName);
                // List<IndexSchema> indexes = indexCache.get(tableName);

                dos.writeUTF(tableName);
                dos.writeUTF(dataFile);
                dos.writeInt(schema.getColumnCount());

                // Loop menggunakan kelas Column Anda
                for (Column col : schema.columns()) {
                    dos.writeUTF(col.name());
                    // Konversi enum DataType Anda ke int
                    dos.writeInt(col.type().getValue()); 
                    // Gunakan getLength() dari kelas Column Anda
                    dos.writeInt(col.length());
                }

                // Tulis Blok Indeks
                // dos.writeInt(indexes.size());
                // for (IndexSchema idx : indexes) {
                //     dos.writeUTF(idx.name());
                //     dos.writeUTF(idx.columnName());
                //     dos.writeInt(idx.indexType());
                //     dos.writeUTF(idx.indexFile());
                // }
            }
        }
    }

    public Schema getSchema(String tableName) {
        return schemaCache.get(tableName);
    }
    
    public String getDataFile(String tableName) {
        return dataFileCache.get(tableName);
    }

    // public List<Index> getIndexes(String tableName) {
    //     return indexCache.getOrDefault(tableName, new ArrayList<>());
    // }
    
    public Collection<Schema> getAllSchemas() {
        return schemaCache.values();
    }
}