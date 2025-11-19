package com.apacy.storagemanager;

import com.apacy.common.dto.*;
import com.apacy.common.enums.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Unit test komprehensif untuk StorageManager.
 * Menguji integrasi CatalogManager, BlockManager, Serializer, dan StatsCollector.
 */
class StorageManagerTest {

    private StorageManager storageManager;
    private final String TEST_DIR = "storage_test_data_" + UUID.randomUUID().toString().substring(0, 8);

    // Skema untuk pengujian
    private Schema studentsSchema;
    private Schema coursesSchema;

    /**
     * Set up:
     * 1. Buat direktori tes sementara.
     * 2. Inisialisasi StorageManager (ini akan memuat/membuat catalog).
     * 3. Gunakan createTable() untuk membuat skema "students" dan "courses".
     */
    @BeforeEach
    void setUp() throws Exception {
        // 1. Inisialisasi StorageManager
        storageManager = new StorageManager(TEST_DIR);
        storageManager.initialize(); // Memuat catalog (atau membuat yang kosong)

        // 2. Definisikan dan BUAT skema "students"
        studentsSchema = new Schema(
            "students",
            "students.dat", // Nama file data
            List.of(
                new Column("id", DataType.INTEGER),
                new Column("name", DataType.VARCHAR, 50),
                new Column("gpa", DataType.FLOAT)
            ),
            List.of(
                // Tambahkan info indeks untuk pengujian statistik
                new IndexSchema("idx_id", "id", IndexType.Hash, "students_id.idx")
            )
        );
        storageManager.createTable(studentsSchema);

        // 3. Definisikan dan BUAT skema "courses" (untuk tes multi-tabel)
        coursesSchema = new Schema(
            "courses",
            "courses.dat",
            List.of(
                new Column("course_id", DataType.VARCHAR, 10),
                new Column("credits", DataType.INTEGER)
            ),
            List.of() // Tidak ada indeks
        );
        storageManager.createTable(coursesSchema);
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

    // ========================================================================
    // --- Tes Fungsionalitas CRUD (Read/Write) ---
    // ========================================================================

    @Test
    @DisplayName("Test: Tulis dan Baca Satu Baris (Integrasi CRUD)")
    void testWriteAndReadSingleRow() {
        System.out.println("--- testWriteAndReadSingleRow ---");
        
        // 1. Buat Row
        Map<String, Object> data = Map.of(
            "id", 101,
            "name", "Budi",
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


    @Test
    @DisplayName("Test: Hash Index Lookup (Equality Search on Indexed Column)")
    void testHashIndexLookup() {
        System.out.println("--- testHashIndexLookup ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 10, "name", "Alice", "gpa", 3.2f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 20, "name", "Budi", "gpa", 2.9f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 30, "name", "Charlie", "gpa", 3.8f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 20, "name", "DuplicateBudi", "gpa", 3.1f)), null));

        DataRetrieval indexLookup = new DataRetrieval(
                "students",
                List.of("*"),
                "id=20",
                true
        );

        List<Row> indexResults = storageManager.readBlock(indexLookup);
        indexResults.forEach(r -> System.out.println("  -> " + r.data()));

        assertEquals(2, indexResults.size(), "Should find exactly 2 rows with id=20");

        boolean found1 = indexResults.stream()
                .anyMatch(r -> r.data().get("name").equals("Budi"));

        boolean found2 = indexResults.stream()
                .anyMatch(r -> r.data().get("name").equals("DuplicateBudi"));

        assertTrue(found1, "Index lookup must return original row with id=20");
        assertTrue(found2, "Index lookup must return duplicate row with id=20");

        StorageManager sm2 = new StorageManager(TEST_DIR);
        sm2.initialize();

        DataRetrieval persistedLookup = new DataRetrieval(
                "students",
                List.of("*"),
                "id=20",
                true
        );

        List<Row> persistedResults = sm2.readBlock(persistedLookup);

        persistedResults.forEach(r -> System.out.println("  (persisted) -> " + r.data()));

        assertEquals(2, persistedResults.size(), "Persistent index must return 2 rows with id=20 after reload");

        System.out.println("Hash index lookup PASSED.");
    }

    @Test
    @DisplayName("Test: Baca dari Tabel Kosong")
    void testReadFromEmptyTable() {
         // Langsung baca dari 'courses' yang baru dibuat (pasti kosong)
        DataRetrieval readReq = new DataRetrieval("courses", List.of("*"), null, false);
        List<Row> results = storageManager.readBlock(readReq);
        
        assertNotNull(results, "Hasil baca tidak boleh null");
        assertEquals(0, results.size(), "Membaca dari tabel kosong harus mengembalikan list kosong");
    }

    @Test
    @DisplayName("Test: Proyeksi (Kolom) saat Read")
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

    @Test
    @DisplayName("Test: Penulisan Multi-Blok (Memicu Append Block)")
    void testWriteCausesBlockAppend() throws IOException {
        System.out.println("--- testWriteCausesBlockAppend ---");

        // Ukuran blok default adalah 4096 bytes.
        // Header blok 8 bytes, Slot 8 bytes.
        
        // Buat data besar (String 3000 char -> 4 byte prefix + 3000 byte data)
        String longName1 = "A".repeat(3000);
        // Ukuran row1 ~ 4 (id) + (4 + 3000) (name) + 4 (gpa) = 3012 bytes
        // Ukuran slot1 = 8 bytes. Total = 3020 bytes. Sisa spasi = 4096 - 8 (header) - 3020 = 1068
        Row row1 = new Row(Map.of("id", 1, "name", longName1, "gpa", 1.0f));
        
        // Buat data kedua yang PASTI tidak muat
        String longName2 = "B".repeat(1500);
        // Ukuran row2 ~ 4 (id) + (4 + 1500) (name) + 4 (gpa) = 1512 bytes
        // Ukuran slot2 = 8 bytes. Total dibutuhkan = 1520 bytes.
        // 1520 > 1068 (sisa spasi), jadi ini akan gagal dan memicu appendBlock.
        Row row2 = new Row(Map.of("id", 2, "name", longName2, "gpa", 2.0f));

        // Tulis blok pertama
        storageManager.writeBlock(new DataWrite("students", row1, null));
        
        // Cek dulu: harus ada 1 blok
        String dataFile = studentsSchema.dataFile();
        assertEquals(1, storageManager.getBlockManager().getBlockCount(dataFile), "Harus ada 1 blok data setelah row pertama");

        // Tulis blok kedua (ini akan memicu append)
        storageManager.writeBlock(new DataWrite("students", row2, null));

        // Cek lagi: harus ada 2 blok
        assertEquals(2, storageManager.getBlockManager().getBlockCount(dataFile), "Harus ada 2 blok data setelah row kedua");

        // Cek apakah kedua data bisa dibaca kembali (Full Table Scan)
        List<Row> results = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(2, results.size(), "Harus ada 2 baris total dari 2 blok");
        
        // Verifikasi datanya
        assertTrue(results.stream().anyMatch(r -> r.data().get("id").equals(1) && r.data().get("name").equals(longName1)));
        assertTrue(results.stream().anyMatch(r -> r.data().get("id").equals(2) && r.data().get("name").equals(longName2)));
    }


    // ========================================================================
    // --- Tes Fungsionalitas Statistik (get_stats) ---
    // ========================================================================

    @Test
    @DisplayName("Test: getAllStats pada Tabel Kosong")
    void testGetAllStats_EmptyTables() {
        System.out.println("--- testGetAllStats_EmptyTables ---");
        
        Map<String, Statistic> statsMap = storageManager.getAllStats();
        
        assertNotNull(statsMap);
        assertTrue(statsMap.containsKey("students"), "Harus ada statistik untuk 'students'");
        assertTrue(statsMap.containsKey("courses"), "Harus ada statistik untuk 'courses'");
        
        // Verifikasi 'students' (kosong)
        Statistic studentStats = statsMap.get("students");
        assertEquals(0, studentStats.nr(), "nr (jumlah row) harus 0");
        // Catatan: createTable() membuat 1 blok header kosong
        assertEquals(1, studentStats.br(), "br (jumlah blok) harus 1 (blok header awal)");
        assertEquals(0, studentStats.lr(), "lr (ukuran tuple) harus 0");
        assertEquals(0, studentStats.fr(), "fr (blocking factor) harus 0");
        
        // Verifikasi 'courses' (kosong)
        Statistic courseStats = statsMap.get("courses");
        assertEquals(0, courseStats.nr(), "courses: nr harus 0");
        assertEquals(1, courseStats.br(), "courses: br harus 1");
    }

    @Test
    @DisplayName("Test: getAllStats dengan Data (nr, br, V(A,r), lr, fr)")
    void testGetAllStats_WithData() {
        System.out.println("--- testGetAllStats_WithData ---");

        // Tulis 3 baris ke 'students'
        storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 1, "name", "Ani", "gpa", 4.0f)), null));
        storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 2, "name", "Budi", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 3, "name", "Ani", "gpa", 3.8f)), null)); // Nama 'Ani' duplikat

        // Tulis 2 baris ke 'courses'
        storageManager.writeBlock(new DataWrite("courses", new Row(Map.of("course_id", "IF3140", "credits", 3)), null));
        storageManager.writeBlock(new DataWrite("courses", new Row(Map.of("course_id", "IF3110", "credits", 4)), null));

        // Panggil getAllStats
        Map<String, Statistic> statsMap = storageManager.getAllStats();

        // --- Verifikasi 'students' ---
        Statistic studentStats = statsMap.get("students");
        // nr: jumlah tuple [cite: 795]
        assertEquals(3, studentStats.nr(), "students: nr (jumlah row) harus 3");
        // br: jumlah blok [cite: 796]
        assertEquals(1, studentStats.br(), "students: br (jumlah blok) harus 1 (3 row kecil muat)");

        // V(A,r): jumlah nilai distinct [cite: 799]
        assertNotNull(studentStats.V());
        assertEquals(3, studentStats.V().get("id"), "students: V(id) harus 3");
        assertEquals(2, studentStats.V().get("name"), "students: V(name) harus 2 ('Ani' duplikat)");
        assertEquals(3, studentStats.V().get("gpa"), "students: V(gpa) harus 3");

        // lr: ukuran tuple (rata-rata) [cite: 797]
        // Perkiraan ukuran:
        // Row1: id(4) + name(4+3) + gpa(4) = 15
        // Row2: id(4) + name(4+4) + gpa(4) = 16
        // Row3: id(4) + name(4+3) + gpa(4) = 15
        // Total = 46. Rata-rata (lr) = 46/3 = 15 (integer division)
        int expected_lr = 15; 
        assertEquals(expected_lr, studentStats.lr(), "students: lr (ukuran rata-rata) harus sekitar 15");
        
        // fr: blocking factor [cite: 798]
        // fr = blockSize / lr = 4096 / 15 = 273
        int expected_fr = (expected_lr == 0) ? 0 : (BlockManager.DEFAULT_BLOCK_SIZE / expected_lr);
        assertEquals(expected_fr, studentStats.fr(), "students: fr (blocking factor) harus 273");
        
        // Cek info indeks (dari DTO, bukan dari spek get_stats)
        assertNotNull(studentStats.indexedColumn());
        assertTrue(studentStats.indexedColumn().containsKey("id"));
        assertEquals(IndexType.Hash, studentStats.indexedColumn().get("id"));

        // --- Verifikasi 'courses' ---
        Statistic courseStats = statsMap.get("courses");
        assertEquals(2, courseStats.nr(), "courses: nr harus 2");
        assertEquals(1, courseStats.br(), "courses: br harus 1");
        assertEquals(2, courseStats.V().get("course_id"), "courses: V(course_id) harus 2");
        assertEquals(2, courseStats.V().get("credits"), "courses: V(credits) harus 2");
        assertTrue(courseStats.indexedColumn().isEmpty(), "courses: indexedColumn harus kosong");
    }


    // ========================================================================
    // --- Tes Error Handling & Stubbed Methods (Wajib) ---
    // ========================================================================

    @Test
    @DisplayName("Test: Operasi pada Tabel Non-Eksisten")
    void testOperationsOnNonExistentTable() {
        System.out.println("--- testOperationsOnNonExistentTable ---");
        
        DataRetrieval readReq = new DataRetrieval("nonexistent", List.of("*"), null, false);
        // readBlock harus mengembalikan list kosong dan mencetak error, bukan melempar exception
        assertDoesNotThrow(() -> {
            List<Row> results = storageManager.readBlock(readReq);
            assertEquals(0, results.size());
        }, "readBlock dari tabel non-eksisten harus mengembalikan list kosong");

        DataWrite writeReq = new DataWrite("nonexistent", new Row(Map.of("col", 1)), null);
        // writeBlock harus mengembalikan 0 dan mencetak error
        assertDoesNotThrow(() -> {
            int affected = storageManager.writeBlock(writeReq);
            assertEquals(0, affected);
        }, "writeBlock ke tabel non-eksisten harus mengembalikan 0");
    }

    @Test
    @DisplayName("Test: Method 'deleteBlock' (Wajib) Melempar UnsupportedOperationException")
    void testDeleteBlockThrowsException() {
        System.out.println("--- testDeleteBlockThrowsException ---");
        
        DataDeletion delReq = new DataDeletion("students", "id=1"); // Filter dummy
        
        // Memverifikasi bahwa implementasi default melempar error
        assertThrows(UnsupportedOperationException.class, () -> {
            storageManager.deleteBlock(delReq);
        }, "Implementasi deleteBlock (wajib) harusnya di-disable atau belum dibuat");
    }

    @Test
    @DisplayName("Test: Method 'setIndex' (Wajib) Melempar UnsupportedOperationException")
    void testSetIndexThrowsException() {
        System.out.println("--- testSetIndexThrowsException ---");

        // Memverifikasi bahwa implementasi default melempar error
        assertThrows(UnsupportedOperationException.class, () -> {
            storageManager.setIndex("students", "name", "BPlusTree");
        }, "Implementasi setIndex (wajib) harusnya di-disable atau belum dibuat");
    }
}
