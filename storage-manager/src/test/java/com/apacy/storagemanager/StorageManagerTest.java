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
 * Menguji integrasi CatalogManager, BlockManager, Serializer, dan
 * StatsCollector.
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
                        new Column("gpa", DataType.FLOAT)),
                List.of(
                        // Tambahkan info indeks untuk pengujian statistik
                        new IndexSchema("idx_id", "id", IndexType.Hash, "students_id.idx"),
                        new IndexSchema("idx_gpa", "gpa", IndexType.BPlusTree, "students_gpa.idx"),
                        new IndexSchema("idx_name", "name", IndexType.BPlusTree, "students_name.idx")));
        storageManager.createTable(studentsSchema);

        // 3. Definisikan dan BUAT skema "courses" (untuk tes multi-tabel)
        coursesSchema = new Schema(
                "courses",
                "courses.dat",
                List.of(
                        new Column("course_id", DataType.VARCHAR, 10),
                        new Column("credits", DataType.INTEGER)),
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
                "gpa", 3.5f);
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
                true);

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
                true);

        List<Row> persistedResults = sm2.readBlock(persistedLookup);

        persistedResults.forEach(r -> System.out.println("  (persisted) -> " + r.data()));

        assertEquals(2, persistedResults.size(), "Persistent index must return 2 rows with id=20 after reload");

        System.out.println("Hash index lookup PASSED.");
    }

    @Test
    @DisplayName("Test: B+Tree Index Lookup (Range Search on GPA Column)")
    void testBPlusIndexLookup() {
        System.out.println("--- testBPlusIndexLookup ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 1, "name", "Alice", "gpa", 3.2f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 2, "name", "Budi", "gpa", 3.5f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 3, "name", "Charlie", "gpa", 3.5f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 4, "name", "Dina", "gpa", 2.7f)), null));

        DataRetrieval indexLookup = new DataRetrieval(
                "students",
                List.of("*"),
                "gpa=3.5",
                true);

        List<Row> indexResults = storageManager.readBlock(indexLookup);
        indexResults.forEach(r -> System.out.println("  -> " + r.data()));

        assertEquals(2, indexResults.size(), "Should find exactly 2 rows with gpa=3.5");

        boolean found1 = indexResults.stream()
                .anyMatch(r -> r.data().get("name").equals("Budi"));

        boolean found2 = indexResults.stream()
                .anyMatch(r -> r.data().get("name").equals("Charlie"));

        assertTrue(found1, "Index lookup must return row with gpa=3.5 (Budi)");
        assertTrue(found2, "Index lookup must return row with gpa=3.5 (Charlie)");

        StorageManager sm2 = new StorageManager(TEST_DIR);
        sm2.initialize();

        DataRetrieval persistedLookup = new DataRetrieval(
                "students",
                List.of("*"),
                "gpa=3.5",
                true);

        List<Row> persistedResults = sm2.readBlock(persistedLookup);

        persistedResults.forEach(r -> System.out.println("  (persisted) -> " + r.data()));

        assertEquals(2, persistedResults.size(),
                "Persistent B+Tree must return the same 2 rows after reload");

        System.out.println("B+Tree index lookup PASSED.");
    }

    @Test
    @DisplayName("Test: Index Lookup on String Column (name='Alice')")
    void testHashIndexStringLookup() {
        System.out.println("--- testIndexStringLookup ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 1, "name", "Alice", "gpa", 3.1f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 2, "name", "Budi", "gpa", 3.5f)), null));

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 3, "name", "Alice", "gpa", 3.8f)), null));

        DataRetrieval lookup = new DataRetrieval(
                "students",
                List.of("*"),
                "name=Alice",
                true);

        List<Row> rows = storageManager.readBlock(lookup);
        rows.forEach(r -> System.out.println(" -> " + r.data()));

        assertEquals(2, rows.size(), "Should find exactly 2 rows with name='Alice'");

        assertTrue(
                rows.stream().anyMatch(r -> r.data().get("id").equals(1)));
        assertTrue(
                rows.stream().anyMatch(r -> r.data().get("id").equals(3)));

        System.out.println("String index lookup PASSED.");
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
        // Ukuran slot1 = 8 bytes. Total = 3020 bytes. Sisa spasi = 4096 - 8 (header) -
        // 3020 = 1068
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
        assertEquals(1, storageManager.getBlockManager().getBlockCount(dataFile),
                "Harus ada 1 blok data setelah row pertama");

        // Tulis blok kedua (ini akan memicu append)
        storageManager.writeBlock(new DataWrite("students", row2, null));

        // Cek lagi: harus ada 2 blok
        assertEquals(2, storageManager.getBlockManager().getBlockCount(dataFile),
                "Harus ada 2 blok data setelah row kedua");

        // Cek apakah kedua data bisa dibaca kembali (Full Table Scan)
        List<Row> results = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(2, results.size(), "Harus ada 2 baris total dari 2 blok");

        // Verifikasi datanya
        assertTrue(
                results.stream().anyMatch(r -> r.data().get("id").equals(1) && r.data().get("name").equals(longName1)));
        assertTrue(
                results.stream().anyMatch(r -> r.data().get("id").equals(2) && r.data().get("name").equals(longName2)));
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
        storageManager
                .writeBlock(new DataWrite("students", new Row(Map.of("id", 1, "name", "Ani", "gpa", 4.0f)), null));
        storageManager
                .writeBlock(new DataWrite("students", new Row(Map.of("id", 2, "name", "Budi", "gpa", 3.5f)), null));
        storageManager
                .writeBlock(new DataWrite("students", new Row(Map.of("id", 3, "name", "Ani", "gpa", 3.8f)), null)); // Nama
                                                                                                                    // 'Ani'
                                                                                                                    // duplikat

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
        // readBlock harus mengembalikan list kosong dan mencetak error, bukan melempar
        // exception
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
    @DisplayName("Test: writeBlock reuse slot terhapus sebelum append blok baru")
    void testWriteReusesDeletedSlots() throws IOException {
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 1, "name", "A", "gpa", 3.0f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 2, "name", "B", "gpa", 3.2f)), null));

        assertEquals(1, storageManager.getBlockManager().getBlockCount(studentsSchema.dataFile()));

        int deleted = storageManager.deleteBlock(new DataDeletion("students", "id=1"));
        assertEquals(1, deleted);

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 3, "name", "C", "gpa", 3.4f)), null));

        assertEquals(1, storageManager.getBlockManager().getBlockCount(studentsSchema.dataFile()),
                "Row baru harus reuse slot terhapus tanpa menambah blok");

        List<Row> rows = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(2, rows.size());
        assertTrue(rows.stream().anyMatch(r -> r.data().get("id").equals(2)));
        assertTrue(rows.stream().anyMatch(r -> r.data().get("id").equals(3)));
        assertFalse(rows.stream().anyMatch(r -> r.data().get("id").equals(1)));
    }

    @Test
    @DisplayName("Test: deleteBlock menandai slot terhapus dan update indeks")
    void testDeleteBlockRemovesRowsAndUpdatesIndexes() {
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 10, "name", "X", "gpa", 3.0f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 20, "name", "Y", "gpa", 2.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 30, "name", "Z", "gpa", 3.5f)), null));

        int deleted = storageManager.deleteBlock(new DataDeletion("students", "id=20"));
        assertEquals(1, deleted);

        List<Row> fullScan = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(2, fullScan.size());
        assertFalse(fullScan.stream().anyMatch(r -> r.data().get("id").equals(20)));

        List<Row> indexScan = storageManager.readBlock(new DataRetrieval("students", List.of("*"), "id=20", true));
        assertEquals(0, indexScan.size(), "Index harus ikut ter-update saat delete");
    }

    @Test
    @DisplayName("Test: setIndex membuat indeks baru dan bisa dipakai setelah restart")
    void testSetIndexCreatesUsableIndex() throws Exception {
        storageManager.writeBlock(new DataWrite("courses",
                new Row(Map.of("course_id", "IF101", "credits", 3)), null));
        storageManager.writeBlock(new DataWrite("courses",
                new Row(Map.of("course_id", "IF102", "credits", 4)), null));
        storageManager.writeBlock(new DataWrite("courses",
                new Row(Map.of("course_id", "IF103", "credits", 3)), null));

        storageManager.setIndex("courses", "credits", "Hash");

        StorageManager sm2 = new StorageManager(TEST_DIR);
        sm2.initialize();
        try {
            List<Row> credit3 = sm2.readBlock(new DataRetrieval("courses", List.of("*"), "credits=3", true));
            assertEquals(2, credit3.size());
            assertTrue(credit3.stream().allMatch(r -> r.data().get("credits").equals(3)));

            List<Row> credit4 = sm2.readBlock(new DataRetrieval("courses", List.of("*"), "credits=4", true));
            assertEquals(1, credit4.size());
            assertEquals(4, credit4.get(0).data().get("credits"));
        } finally {
            sm2.shutdown();
        }
    }

    // ========================================================================
    // --- Tes Komparasi & Filter (Equality dan Inequality) ---
    // ========================================================================

    @Test
    @DisplayName("Test: Equality Filter (=) pada INTEGER column")
    void testEqualityFilterInteger() {
        System.out.println("--- testEqualityFilterInteger ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 100, "name", "Alice", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 200, "name", "Bob", "gpa", 2.9f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 100, "name", "Charlie", "gpa", 3.8f)), null));

        // Cari id=100 (duplikat)
        DataRetrieval query = new DataRetrieval("students", List.of("*"), "id=100", false);
        List<Row> results = storageManager.readBlock(query);

        assertEquals(2, results.size(), "Harus ada 2 baris dengan id=100");
        assertTrue(results.stream().allMatch(r -> r.data().get("id").equals(100)));
        assertTrue(results.stream().anyMatch(r -> r.data().get("name").equals("Alice")));
        assertTrue(results.stream().anyMatch(r -> r.data().get("name").equals("Charlie")));

        // Cari id=200 (single)
        query = new DataRetrieval("students", List.of("*"), "id=200", false);
        results = storageManager.readBlock(query);

        assertEquals(1, results.size(), "Harus ada 1 baris dengan id=200");
        assertEquals("Bob", results.get(0).data().get("name"));

        // Cari id=999 (tidak ada)
        query = new DataRetrieval("students", List.of("*"), "id=999", false);
        results = storageManager.readBlock(query);

        assertEquals(0, results.size(), "Tidak boleh ada baris dengan id=999");
    }

    @Test
    @DisplayName("Test: Equality Filter (=) pada VARCHAR column")
    void testEqualityFilterVarchar() {
        System.out.println("--- testEqualityFilterVarchar ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 1, "name", "Alice", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 2, "name", "Bob", "gpa", 2.9f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 3, "name", "Alice", "gpa", 3.2f)), null));

        // Cari name='Alice'
        DataRetrieval query = new DataRetrieval("students", List.of("*"), "name=Alice", false);
        List<Row> results = storageManager.readBlock(query);

        assertEquals(2, results.size(), "Harus ada 2 baris dengan name='Alice'");
        assertTrue(results.stream().allMatch(r -> r.data().get("name").equals("Alice")));

        // Cari name='Bob'
        query = new DataRetrieval("students", List.of("*"), "name=Bob", false);
        results = storageManager.readBlock(query);

        assertEquals(1, results.size(), "Harus ada 1 baris dengan name='Bob'");
        assertEquals(2, results.get(0).data().get("id"));
    }

    @Test
    @DisplayName("Test: Equality Filter (=) pada FLOAT column")
    void testEqualityFilterFloat() {
        System.out.println("--- testEqualityFilterFloat ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 1, "name", "Alice", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 2, "name", "Bob", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 3, "name", "Charlie", "gpa", 2.7f)), null));

        // Cari gpa=3.5
        DataRetrieval query = new DataRetrieval("students", List.of("*"), "gpa=3.5", false);
        List<Row> results = storageManager.readBlock(query);

        assertEquals(2, results.size(), "Harus ada 2 baris dengan gpa=3.5");
        assertTrue(results.stream().allMatch(r -> r.data().get("gpa").equals(3.5f)));
    }

    @Test
    @DisplayName("Test: Multiple Filters dengan AND (Kombinasi)")
    void testMultipleFiltersWithAnd() {
        System.out.println("--- testMultipleFiltersWithAnd ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 10, "name", "Alice", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 20, "name", "Alice", "gpa", 2.9f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 30, "name", "Bob", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 40, "name", "Bob", "gpa", 2.9f)), null));

        // Query: name=Alice AND gpa=3.5
        DataRetrieval query = new DataRetrieval("students", List.of("*"), "name=Alice AND gpa=3.5", false);
        List<Row> results = storageManager.readBlock(query);

        assertEquals(1, results.size(), "Hanya 1 baris match name=Alice AND gpa=3.5");
        assertEquals(10, results.get(0).data().get("id"));

        // Query: name=Bob AND gpa=2.9
        query = new DataRetrieval("students", List.of("*"), "name=Bob AND gpa=2.9", false);
        results = storageManager.readBlock(query);

        assertEquals(1, results.size(), "Hanya 1 baris match name=Bob AND gpa=2.9");
        assertEquals(40, results.get(0).data().get("id"));

        // Query: id=99 AND name=Alice (tidak ada match)
        query = new DataRetrieval("students", List.of("*"), "id=99 AND name=Alice", false);
        results = storageManager.readBlock(query);

        assertEquals(0, results.size(), "Tidak boleh ada match untuk kondisi yang tidak terpenuhi");
    }

    @Test
    @DisplayName("Test: Project Columns dengan Filter")
    void testProjectionWithFilter() {
        System.out.println("--- testProjectionWithFilter ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 1, "name", "Alice", "gpa", 3.5f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 2, "name", "Bob", "gpa", 2.9f)), null));

        // Query: id, name ONLY, dengan filter name=Alice
        DataRetrieval query = new DataRetrieval("students", List.of("id", "name"), "name=Alice", false);
        List<Row> results = storageManager.readBlock(query);

        assertEquals(1, results.size());
        Row result = results.get(0);
        assertEquals(2, result.data().size(), "Hanya 2 kolom yang diproyeksikan");
        assertTrue(result.data().containsKey("id"));
        assertTrue(result.data().containsKey("name"));
        assertFalse(result.data().containsKey("gpa"), "gpa tidak boleh ada (terproyeksi keluar)");
    }

    @Test
    @DisplayName("Test: Large Dataset (300+ rows)")
    void testLargeDataset() throws IOException {
        System.out.println("--- testLargeDataset ---");

        // Tulis 300 baris (untuk memastikan > 1 blok dengan data yang lebih besar)
        for (int i = 0; i < 300; i++) {
            String name = (i % 3 == 0) ? "AliceMediumName" : (i % 3 == 1) ? "BobMediumName" : "CharlieMediumName";
            float gpa = 2.0f + (i % 20) * 0.1f;
            Row row = new Row(Map.of("id", i, "name", name, "gpa", gpa));
            storageManager.writeBlock(new DataWrite("students", row, null));
        }

        // Verifikasi jumlah blok (300 rows dengan nama panjang harus > 1 blok)
        long blockCount = storageManager.getBlockManager().getBlockCount("students.dat");
        assertTrue(blockCount > 1, "300 baris harus lebih dari 1 blok");

        // Full scan
        List<Row> allRows = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(300, allRows.size(), "Full scan harus mengembalikan 300 baris");

        // Filter
        List<Row> aliceRows = storageManager.readBlock(new DataRetrieval("students", List.of("*"), "name=AliceMediumName", false));
        assertEquals(100, aliceRows.size(), "Ada 100 baris dengan name=AliceMediumName (300/3)");

        // Filter 2
        List<Row> gpaBetween = storageManager
                .readBlock(new DataRetrieval("students", List.of("id", "gpa"), "name=BobMediumName", false));
        assertEquals(100, gpaBetween.size(), "Ada 100 baris dengan name=BobMediumName");
    }

    @Test
    @DisplayName("Test: GetSchema untuk tabel yang ada")
    void testGetSchema() {
        Schema schema = storageManager.getSchema("students");
        assertNotNull(schema, "Schema untuk 'students' harus ada");
        assertEquals("students", schema.tableName());
        assertEquals("students.dat", schema.dataFile());
        assertEquals(3, schema.columns().size(), "students punya 3 kolom");

        Schema notFound = storageManager.getSchema("nonexistent");
        assertNull(notFound, "Schema untuk tabel non-eksisten harus null");
    }

    @Test
    @DisplayName("Test: Concurrent Read & Write (Basic)")
    void testConcurrentReadWrite() throws InterruptedException {
        System.out.println("--- testConcurrentReadWrite ---");

        // Thread 1: Write 50 rows
        Thread writerThread = new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                Row row = new Row(Map.of("id", 1000 + i, "name", "Writer" + i, "gpa", 3.0f + (i % 10) * 0.1f));
                storageManager.writeBlock(new DataWrite("students", row, null));
            }
        });

        // Thread 2: Read rows (after initial write)
        Thread readerThread = new Thread(() -> {
            try {
                Thread.sleep(50); // Wait untuk writer memulai
                for (int i = 0; i < 5; i++) {
                    List<Row> rows = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
                    assertTrue(rows.size() >= 0, "Read harus selalu berhasil");
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        writerThread.start();
        readerThread.start();

        writerThread.join();
        readerThread.join();

        List<Row> finalRows = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(50, finalRows.size(), "Harus ada 50 baris dari writer thread");
    }

    @Test
    @DisplayName("Test: Delete dengan Multiple Matches")
    void testDeleteMultipleMatches() {
        System.out.println("--- testDeleteMultipleMatches ---");

        // Insert data dengan duplikat
        for (int i = 0; i < 10; i++) {
            Row row = new Row(Map.of("id", i % 3, "name", "Name" + i, "gpa", 3.0f + i * 0.1f));
            storageManager.writeBlock(new DataWrite("students", row, null));
        }

        List<Row> allRows = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(10, allRows.size(), "Awalnya ada 10 baris");

        // Delete semua dengan id=0
        int deleted = storageManager.deleteBlock(new DataDeletion("students", "id=0"));
        assertEquals(4, deleted, "Harus delete 4 baris dengan id=0 (index 0, 3, 6, 9)");

        // Verifikasi
        List<Row> remaining = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(6, remaining.size(), "Harus sisa 6 baris");
        assertFalse(remaining.stream().anyMatch(r -> r.data().get("id").equals(0)), "Tidak boleh ada id=0");
    }

    @Test
    @DisplayName("Test: Empty Filter (null/empty string) returns all rows")
    void testEmptyFilterReturnsAll() {
        System.out.println("--- testEmptyFilterReturnsAll ---");

        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 1, "name", "A", "gpa", 3.0f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 2, "name", "B", "gpa", 3.1f)), null));
        storageManager.writeBlock(new DataWrite("students",
                new Row(Map.of("id", 3, "name", "C", "gpa", 3.2f)), null));

        // null filter
        List<Row> results = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
        assertEquals(3, results.size(), "null filter harus return semua baris");

        // empty string filter
        results = storageManager.readBlock(new DataRetrieval("students", List.of("*"), "", false));
        assertEquals(3, results.size(), "empty string filter harus return semua baris");
    }

    @Test
    @DisplayName("Test: Statistics after large inserts and deletes")
    void testStatisticsAfterLargeOperations() {
        System.out.println("--- testStatisticsAfterLargeOperations ---");

        // Insert 100 rows
        for (int i = 0; i < 100; i++) {
            Row row = new Row(Map.of("id", i, "name", "Item" + (i % 10), "gpa", 2.0f + (i % 40) * 0.05f));
            storageManager.writeBlock(new DataWrite("students", row, null));
        }

        Map<String, Statistic> stats1 = storageManager.getAllStats();
        assertEquals(100, stats1.get("students").nr(), "After 100 inserts, nr should be 100");
        assertEquals(10, stats1.get("students").V().get("name"), "Should have 10 distinct names");

        // Delete 50 rows
        for (int i = 0; i < 50; i++) {
            storageManager.deleteBlock(new DataDeletion("students", "id=" + i));
        }

        // Re-compute stats
        Map<String, Statistic> stats2 = storageManager.getAllStats();
        assertEquals(50, stats2.get("students").nr(), "After deleting 50, nr should be 50");
    }
}
