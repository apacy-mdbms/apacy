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
 * PERBAIKAN: Disesuaikan untuk HANYA menggunakan satu 'schemaCache'.
 */
public class CatalogManager {

    private final String catalogFilePath;
    private final int MAGIC_NUMBER = 0xACDB0101; 

    // PERBAIKAN: Hanya satu cache yang diperlukan
    private Map<String, Schema> schemaCache;

    // ‼️ Cache terpisah DIHAPUS
    // private Map<String, String> dataFileCache;
    // private Map<String, List<IndexSchema>> indexCache;

    public CatalogManager(String catalogFilePath) {
        this.catalogFilePath = catalogFilePath;
        this.schemaCache = new HashMap<>();
        // ‼️ Inisialisasi cache lain DIHAPUS
    }

    /**
     * Dipanggil oleh StorageManager.initialize() saat startup.
     */
    public void loadCatalog() throws IOException {
        System.out.println("CatalogManager: Memuat katalog dari " + catalogFilePath + "...");
        this.schemaCache.clear();

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

                // Baca Kolom
                List<Column> columns = new ArrayList<>();
                for (int j = 0; j < columnCount; j++) {
                    String colName = dis.readUTF();
                    int colTypeInt = dis.readInt();
                    int colLength = dis.readInt();
                    DataType colType = DataType.fromValue(colTypeInt); 
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
                
                // PERBAIKAN: Simpan ke cache tunggal
                this.schemaCache.put(tableName, schema);
                System.out.println("CatalogManager: Memuat skema untuk tabel '" + tableName + "'.");
            }
        } catch (FileNotFoundException e) {
            System.out.println("CatalogManager: Katalog '" + catalogFilePath + "' belum ada. Membuat katalog awal...");
            createSystemCatalog(); // Panggil helper untuk membuat file
        }
        System.out.println("CatalogManager: Katalog berhasil dimuat ke memori.");
    }

    /**
     * Menulis ulang seluruh cache memori ke file 'system_catalog.dat'.
     */
    public void writeCatalog() throws IOException {
        System.out.println("CatalogManager: Menulis ulang katalog ke disk...");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(catalogFilePath))) {
            
            dos.writeInt(MAGIC_NUMBER);
            dos.writeInt(schemaCache.size()); 

            // PERBAIKAN: Loop pada 'schemaCache.values()'
            for (Schema schema : schemaCache.values()) {
                
                // Ambil data HANYA dari objek Schema
                dos.writeUTF(schema.tableName());
                dos.writeUTF(schema.dataFile());
                dos.writeInt(schema.columns().size());

                // Tulis Kolom
                for (Column col : schema.columns()) {
                    dos.writeUTF(col.name());
                    dos.writeInt(col.type().getValue()); 
                    dos.writeInt(col.length());
                }

                // Tulis Indeks
                dos.writeInt(schema.indexes().size());
                for (IndexSchema idx : schema.indexes()) {
                    dos.writeUTF(idx.indexName());
                    dos.writeUTF(idx.columnName());
                    dos.writeInt(idx.indexType().getValue());
                    dos.writeUTF(idx.indexFile());
                }
            }
        }
    }

    // --- GETTER ---
    public Schema getSchema(String tableName) {
        return schemaCache.get(tableName);
    }
    
    // ‼️ 'getDataFile' dan 'getIndexes' DIHAPUS (info sudah ada di Schema)
    // public String getDataFile(String tableName) { ... }
    // public List<IndexSchema> getIndexes(String tableName) { ... }
    
    public Collection<Schema> getAllSchemas() {
        return schemaCache.values();
    }

    // --- LOGIKA CREATE TABLE ---
    
    public void addSchemaToCache(Schema newSchema) throws IOException {
        if (schemaCache.containsKey(newSchema.tableName())) {
            throw new IOException("Tabel '" + newSchema.tableName() + "' sudah ada.");
        }
        // PERBAIKAN: Hanya perlu menambah ke satu cache
        schemaCache.put(newSchema.tableName(), newSchema);
    }

    /**
     * Utilitas untuk membuat file katalog awal (kosong) jika tidak ada.
     */
    private void createSystemCatalog() throws IOException {
        System.out.println("Membuat file katalog awal (kosong) di: " + catalogFilePath);
        new File(catalogFilePath).getParentFile().mkdirs();
        
        // Panggil writeCatalog() saat cache masih kosong (tableCount=0)
        writeCatalog();
    }

    /**
     * Update schema in cache
     * @param updatedSchema
     * @throws IOException
     */
    public void updateSchema(Schema updatedSchema) throws IOException {
        if (!schemaCache.containsKey(updatedSchema.tableName())) {
            throw new IOException("Table: " + updatedSchema.tableName() + " not found");
        }
        schemaCache.put(updatedSchema.tableName(), updatedSchema);
    }

    /**
     * Main method untuk membuat file katalog dummy secara manual.
     */
    public static void main(String[] args) throws IOException {
        // PERBAIKAN: Path harus menunjuk ke file, bukan direktori
        CatalogManager catalogManager = new CatalogManager("storage-manager/data/system_catalog.dat"); 
        catalogManager.createSystemCatalog();
    }
}