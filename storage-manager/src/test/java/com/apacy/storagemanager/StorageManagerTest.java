package com.apacy.storagemanager;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.enums.*;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Statistic;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

class StorageManagerTest {

    private StorageManager storageManager;
    private CatalogManager catalogManager;
    private final String TEST_DIR = "storage_test_data_" + UUID.randomUUID().toString().substring(0, 8);
    private final String TEST_CATALOG_FILE = TEST_DIR + "/system_catalog.dat";
    private final String TEST_STUDENT_FILE = TEST_DIR + "/students.dat";

    /**
     * Set up:
     * 1. Buat direktori tes sementara.
     * 2. Buat file system_catalog.dat BINER palsu (dummy).
     * 3. Inisialisasi StorageManager, yang akan memuat katalog palsu tsb.
     */
    @BeforeEach
    void setUp() throws Exception {
        // 1. Buat direktori
        new File(TEST_DIR).mkdirs();

        // 2. Buat file system_catalog.dat biner palsu
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(TEST_CATALOG_FILE))) {
            // Header Katalog
            dos.writeInt(0xACDB01); // Magic Number
            dos.writeInt(1); // Jumlah Tabel = 1

            // --- Definisi Tabel "students" ---
            dos.writeUTF("students"); // Nama Tabel
            dos.writeUTF(TEST_STUDENT_FILE); // Data File
            dos.writeInt(3); // Jumlah Kolom = 3

            // Kolom 1: id (INTEGER)
            dos.writeUTF("id");
            dos.writeInt(DataType.INTEGER.getValue());
            dos.writeInt(0); // Length (0 untuk int)

            // Kolom 2: name (VARCHAR)
            dos.writeUTF("name");
            dos.writeInt(DataType.VARCHAR.getValue());
            dos.writeInt(50); // Length 50

            // Kolom 3: gpa (FLOAT)
            dos.writeUTF("gpa");
            dos.writeInt(DataType.FLOAT.getValue());
            dos.writeInt(0); // Length (0 untuk float)

            // Indeks
            dos.writeInt(0); // Jumlah Indeks = 0
        }

        // 3. Inisialisasi StorageManager
        // Ini akan memanggil initialize() -> catalogManager.loadCatalog()
        storageManager = new StorageManager(TEST_DIR);
        storageManager.initialize();
        
        // Simpan referensi ke katalog untuk pengujian
        catalogManager = storageManager.getCatalogManager();
    }

    /**
     * Teardown:
     * 1. Hapus direktori tes dan semua isinya.
     */
    @AfterEach
    void tearDown() throws IOException {
        storageManager.shutdown();
        deleteDirectory(new File(TEST_DIR));
    }

    /**
     * Tes Integrasi Inti: Tulis satu baris, lalu baca kembali (Full Scan).
     * Ini menguji: CatalogManager, Serializer(pack), BlockManager(write),
     * BlockManager(read), Serializer(deserializeBlock), dan StorageManager(readBlock).
     */
    @Test
    void testWriteAndReadSingleRow() {
        System.out.println("--- testWriteAndReadSingleRow ---");
        
        // 1. Buat Row (Data Variabel)
        Map<String, Object> data = Map.of(
            "id", 101,
            "name", "Budi", // String pendek
            "gpa", 3.5f
        );
        Row newRow = new Row(data);
        DataWrite writeReq = new DataWrite("students", newRow, null);

        // 2. Tulis ke Blok
        int affectedRows = storageManager.writeBlock(writeReq);
        assertEquals(1, affectedRows, "writeBlock harus mengembalikan 1 baris terpengaruh");

        // 3. Baca kembali (Full Scan)
        DataRetrieval readReq = new DataRetrieval("students", List.of("*"), null, false);
        List<Row> results = storageManager.readBlock(readReq);

        // 4. Verifikasi
        assertNotNull(results, "Hasil baca tidak boleh null");
        assertEquals(1, results.size(), "Harus ada 1 baris di tabel");
        
        Row resultRow = results.get(0);
        assertEquals(101, resultRow.data().get("id"));
        assertEquals("Budi", resultRow.data().get("name"));
        assertEquals(3.5f, resultRow.data().get("gpa"));
    }

    /**
     * Tes penulisan beberapa baris (termasuk logika 'append block').
     */
    @Test
    void testWriteMultipleRows() throws IOException {
        System.out.println("--- testWriteMultipleRows ---");
        
        // Tulis 2 baris
        storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 1, "name", "Row 1", "gpa", 1.0f)), null));
        storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 2, "name", "Row 2", "gpa", 2.0f)), null));

        // Baca kembali
        DataRetrieval readReq = new DataRetrieval("students", List.of("*"), null, false);
        List<Row> results = storageManager.readBlock(readReq);

        assertEquals(2, results.size(), "Harus ada 2 baris");

        // Tes logika 'append block' (jika 1 blok penuh)
        // Buat data besar yang akan memenuhi blok
        String longName = "NamaPanjang".repeat(200); // ~4000+ byte, pasti butuh blok baru
        storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 3, "name", longName, "gpa", 3.0f)), null));

        results = storageManager.readBlock(readReq);
        assertEquals(3, results.size(), "Harus ada 3 baris (termasuk yang di blok baru)");
        
        // Verifikasi baris terakhir
        Row lastRow = results.stream().filter(r -> r.data().get("id").equals(3)).findFirst().get();
        assertEquals(longName, lastRow.data().get("name"));
    }

    /**
     * Tes Proyeksi (Kolom): Memastikan `readBlock` hanya mengembalikan kolom yang diminta.
     */
    @Test
    void testReadWithProjection() {
        System.out.println("--- testReadWithProjection ---");
        
        Map<String, Object> data = Map.of("id", 101, "name", "Budi", "gpa", 3.5f);
        storageManager.writeBlock(new DataWrite("students", new Row(data), null));

        // Minta "id" dan "gpa" SAJA.
        DataRetrieval readReq = new DataRetrieval("students", List.of("id", "gpa"), null, false);
        List<Row> results = storageManager.readBlock(readReq);

        assertEquals(1, results.size());
        Row resultRow = results.get(0);

        // Verifikasi bahwa hanya kolom yang diminta yang ada
        assertEquals(2, resultRow.data().size(), "Hasil Row harusnya hanya punya 2 kolom");
        assertTrue(resultRow.data().containsKey("id"), "Harus ada 'id'");
        assertTrue(resultRow.data().containsKey("gpa"), "Harus ada 'gpa'");
        assertFalse(resultRow.data().containsKey("name"), "'name' seharusnya tidak ada (terproyeksi keluar)");
    }

    // /**
    //  * Tes Statistik: Memastikan `getStats` (Orang 4) berfungsi.
    //  */
    // @Test
    // void testGetStats() {
    //     System.out.println("--- testGetStats ---");

    //     // Tulis 2 baris
    //     storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 1, "name", "Row 1", "gpa", 1.0f)), null));
    //     storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 2, "name", "Row 2", "gpa", 2.0f)), null));
        
    //     // Panggil getStats (yang diimplementasikan oleh Orang 4)
    //     Statistic stats = storageManager.getAllStats();

    //     assertNotNull(stats, "Statistik tidak boleh null");
    //     assertEquals(2, stats.nr(), "nr (jumlah row) harus 2");
    //     assertEquals(1, stats.br(), "br (jumlah blok) harus 1 (karena 2 row kecil muat)");
        
    //     // Cek V(A,r) - Jumlah nilai unik
    //     assertEquals(2, stats.V().get("id"));
    //     assertEquals(2, stats.V().get("name"));
    //     assertEquals(2, stats.V().get("gpa"));
        
    //     // Cek info indeks (dari katalog)
    //     assertEquals(0, stats.indexedColumns().size(), "indexedColumns harus 0 (sesuai katalog palsu)");
    // }

    /**
     * Tes Delete: Memastikan deleteBlock melempar error (karena implementasi rewrite berbahaya).
     * Ganti tes ini jika Anda mengimplementasikan delete-by-slot.
     */
    @Test
    void testDeleteBlockThrowsException() {
        System.out.println("--- testDeleteBlockThrowsException ---");
        
        DataDeletion delReq = new DataDeletion("students", "id=1"); // Filter dummy
        
        // Memverifikasi bahwa implementasi default (berbahaya) melempar error
        assertThrows(UnsupportedOperationException.class, () -> {
            storageManager.deleteBlock(delReq);
        }, "Implementasi deleteBlock (rewrite) harusnya di-disable");
    }

    // --- Helper untuk Teardown ---
    private void deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}