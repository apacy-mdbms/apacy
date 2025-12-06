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

import com.apacy.storagemanager.index.IIndex;
import com.apacy.storagemanager.index.HashIndex;
import com.apacy.common.dto.ast.where.*;
import com.apacy.common.dto.ast.expression.*;

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
  private Schema enrollmentsSchema;

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

    // 4. Definisikan dan BUAT skema "enrollments" (Mengandung Foreign Key ke
    // students.id)
    enrollmentsSchema = new Schema(
        "enrollments",
        "enrollments.dat",
        List.of(
            new Column("student_id", DataType.INTEGER),
            new Column("course_id", DataType.VARCHAR, 10),
            new Column("semester", DataType.VARCHAR, 20)),
        List.of(
            // Indeks pada FK (student_id)
            new IndexSchema("idx_enroll_student", "student_id", IndexType.BPlusTree, "enrollments_student.idx")),
        List.of(
            // MENGGUNAKAN ForeignKeySchema YANG BARU DIDEFINISIKAN
            new ForeignKeySchema(
                "fk_student_id", // constraintName
                "student_id", // columnName (Kolom lokal)
                "students", // referenceTable (Tabel tujuan)
                "id", // referenceColumn (Kolom tujuan)
                false // isCascading: false = RESTRICT (Default)
            )));
    storageManager.createTable(enrollmentsSchema);
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

  // --- Helper untuk membangun WhereConditionNode dari comparison ---
  /**
   * Helper untuk membuat simple comparison: column op value
   * Contoh: buildComparison("id", "=", 20) -> id=20
   */
  private WhereConditionNode buildComparison(String columnName, String operator, Object value) {
    TermNode colTerm = new TermNode(new ColumnFactor(columnName), List.of());
    ExpressionNode leftExpr = new ExpressionNode(colTerm, List.of());
    
    TermNode valTerm = new TermNode(new LiteralFactor(value), List.of());
    ExpressionNode rightExpr = new ExpressionNode(valTerm, List.of());
    
    return new ComparisonConditionNode(leftExpr, operator, rightExpr);
  }

  /**
   * Helper untuk membuat binary condition: left op right (AND/OR)
   */
  private WhereConditionNode buildBinary(WhereConditionNode left, String operator, WhereConditionNode right) {
    return new BinaryConditionNode(left, operator, right);
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

    WhereConditionNode condition = buildComparison("id", "=", 20);
    DataRetrieval indexLookup = new DataRetrieval(
        "students",
        List.of("*"),
        condition,
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

    WhereConditionNode persistedCondition = buildComparison("id", "=", 20);
    DataRetrieval persistedLookup = new DataRetrieval(
        "students",
        List.of("*"),
        persistedCondition,
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

    WhereConditionNode gpaCondition = buildComparison("gpa", "=", 3.5f);
    DataRetrieval indexLookup = new DataRetrieval(
        "students",
        List.of("*"),
        gpaCondition,
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

    WhereConditionNode persistedGpaCondition = buildComparison("gpa", "=", 3.5f);
    DataRetrieval persistedLookup = new DataRetrieval(
        "students",
        List.of("*"),
        persistedGpaCondition,
        true);

    List<Row> persistedResults = sm2.readBlock(persistedLookup);

    persistedResults.forEach(r -> System.out.println("  (persisted) -> " + r.data()));

    assertEquals(2, persistedResults.size(),
        "Persistent B+Tree must return the same 2 rows after reload");

    System.out.println("B+Tree index lookup PASSED.");
  }

  @Test
  @DisplayName("Test: B+Tree Range Search (GPA < 3.0f)")
  void testBPlusIndexRangeSearch_LessThan() {
    System.out.println("--- testBPlusIndexRangeSearch_LessThan ---");

    // Data: (2.7), 2.9, 3.2, 3.5, 3.8
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 1, "name", "A", "gpa", 3.2f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 2, "name", "B", "gpa", 2.9f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 3, "name", "C", "gpa", 3.5f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 4, "name", "D", "gpa", 3.8f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 5, "name", "E", "gpa", 2.7f)), null));

    // Query: gpa < 3.0 (Seharusnya mendapatkan 2.9 dan 2.7)
    WhereConditionNode ltCondition = buildComparison("gpa", "<", 3.0f);
    DataRetrieval indexLookup = new DataRetrieval(
        "students",
        List.of("*"),
        ltCondition,
        true);

    List<Row> results = storageManager.readBlock(indexLookup);
    results.forEach(r -> System.out.println(" -> " + r.data()));

    assertEquals(2, results.size(), "Range search gpa<3.0 harus menemukan 2 baris (2.9, 2.7)");
    assertTrue(results.stream().noneMatch(r -> (float) r.data().get("gpa") >= 3.0f), "Nilai harus < 3.0f");

    System.out.println("Range search gpa<3.0 PASSED.");
  }

  @Test
  @DisplayName("Test: B+Tree Range Search (GPA >= 3.5f) - Inclusive")
  void testBPlusIndexRangeSearch_GreaterThanOrEqual() {
    System.out.println("--- testBPlusIndexRangeSearch_GreaterThanOrEqual ---");

    // Data: 3.2, (3.5, 3.5), 3.8, 4.0
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 10, "name", "X", "gpa", 3.2f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 11, "name", "Y", "gpa", 3.5f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 12, "name", "Z", "gpa", 3.8f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 13, "name", "W", "gpa", 4.0f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 14, "name", "V", "gpa", 3.5f)), null)); // Duplikat

    // Query: gpa >= 3.5 (Seharusnya mendapatkan 3.5 (x2), 3.8, 4.0)
    WhereConditionNode gteCondition = buildComparison("gpa", ">=", 3.5f);
    DataRetrieval indexLookup = new DataRetrieval(
        "students",
        List.of("*"),
        gteCondition,
        true);

    List<Row> results = storageManager.readBlock(indexLookup);
    results.forEach(r -> System.out.println(" -> " + r.data()));

    assertEquals(4, results.size(), "Range search gpa>=3.5 harus menemukan 4 baris");
    assertTrue(results.stream().allMatch(r -> (float) r.data().get("gpa") >= 3.5f), "Semua nilai harus >= 3.5f");

    System.out.println("Range search gpa>=3.5 PASSED.");
  }

  @Test
  @DisplayName("Test: B+Tree Range Search (Range Tertutup: 3.2f < gpa <= 3.8f)")
  void testBPlusIndexRangeSearch_BetweenExclusiveAndInclusive() {
    System.out.println("--- testBPlusIndexRangeSearch_BetweenExclusiveAndInclusive ---");

    // Data: 3.1, 3.2, 3.3, 3.6, 3.8, 3.9
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 20, "name", "A", "gpa", 3.1f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 21, "name", "B", "gpa", 3.2f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 22, "name", "C", "gpa", 3.3f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 23, "name", "D", "gpa", 3.6f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 24, "name", "E", "gpa", 3.8f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 25, "name", "F", "gpa", 3.9f)), null));

    // Query: gpa>3.2 AND gpa<=3.8 (Seharusnya mendapatkan 3.3, 3.6, 3.8)
    WhereConditionNode gtCondition = buildComparison("gpa", ">", 3.2f);
    WhereConditionNode lteCondition = buildComparison("gpa", "<=", 3.8f);
    WhereConditionNode andCondition = buildBinary(gtCondition, "AND", lteCondition);
    DataRetrieval indexLookup = new DataRetrieval(
        "students",
        List.of("*"),
        andCondition,
        true);

    List<Row> results = storageManager.readBlock(indexLookup);
    results.forEach(r -> System.out.println(" -> " + r.data()));

    assertEquals(3, results.size(), "Range search harus menemukan 3 baris (3.3, 3.6, 3.8)");

    // Cek boundary
    assertTrue(results.stream().noneMatch(r -> r.data().get("gpa").equals(3.2f)), "3.2f harus excluded (gpa>3.2)");
    assertTrue(results.stream().anyMatch(r -> r.data().get("gpa").equals(3.8f)), "3.8f harus included (gpa<=3.8)");
    assertTrue(results.stream().noneMatch(r -> r.data().get("gpa").equals(3.1f)), "3.1f harus excluded");
    assertTrue(results.stream().noneMatch(r -> r.data().get("gpa").equals(3.9f)), "3.9f harus excluded");

    System.out.println("Range search gpa>3.2 AND gpa<=3.8 PASSED.");
  }

  @Test
  @DisplayName("Test: B+Tree Range Search (String Range: name >= 'Budi')")
  void testBPlusIndexRangeSearch_StringRange() {
    System.out.println("--- testBPlusIndexRangeSearch_StringRange ---");

    // Data: Alice, Budi, Charlie, Dora
    storageManager
        .writeBlock(new DataWrite("students", new Row(Map.of("id", 30, "name", "Charlie", "gpa", 3.0f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 31, "name", "Alice", "gpa", 3.0f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 32, "name", "Budi", "gpa", 3.0f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 33, "name", "Dora", "gpa", 3.0f)), null));

    // Query: name>='Budi' (Seharusnya mendapatkan Budi, Charlie, Dora)
    WhereConditionNode nameCondition = buildComparison("name", ">=", "Budi");
    DataRetrieval indexLookup = new DataRetrieval(
        "students",
        List.of("*"),
        nameCondition,
        true);

    List<Row> results = storageManager.readBlock(indexLookup);
    results.forEach(r -> System.out.println(" -> " + r.data()));

    assertEquals(3, results.size(), "Range search name>='Budi' harus menemukan 3 baris");
    assertTrue(results.stream().anyMatch(r -> r.data().get("name").equals("Budi")));
    assertTrue(results.stream().noneMatch(r -> r.data().get("name").equals("Alice")));

    System.out.println("String range search PASSED.");
  }

  @Test
  @DisplayName("Test: Delete Rows using B+Tree Range Scan (gpa < 3.0f)")
  void testDeleteWithRangeIndex() {
    System.out.println("--- testDeleteWithRangeIndex ---");

    // Data: 2.5, 2.8, 3.0, 3.5, 4.0
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 10, "name", "X", "gpa", 3.5f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 20, "name", "Y", "gpa", 2.8f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 30, "name", "Z", "gpa", 4.0f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 40, "name", "W", "gpa", 2.5f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 50, "name", "V", "gpa", 3.0f)), null));

    // 1. Delete: gpa < 3.0 (Seharusnya menghapus ID 20, 40)
    WhereConditionNode deleteCondition = buildComparison("gpa", "<", 3.0f);
    DataDeletion deleteReq = new DataDeletion("students", deleteCondition);
    int deleted = storageManager.deleteBlock(deleteReq);
    assertEquals(2, deleted, "Harus menghapus 2 baris (2.8, 2.5)");

    // 2. Verifikasi Full Scan (Cek data file)
    DataRetrieval fullScan = new DataRetrieval("students", List.of("*"), null, false);
    List<Row> remainingRows = storageManager.readBlock(fullScan);
    assertEquals(3, remainingRows.size(), "Harus sisa 3 baris");
    assertTrue(remainingRows.stream().noneMatch(r -> (int) r.data().get("id") == 20), "ID 20 harus terhapus");

    // 3. Verifikasi Index Scan (Cek B+Tree)
    // Cari lagi gpa < 3.0, hasilnya harus 0
    WhereConditionNode checkCondition = buildComparison("gpa", "<", 3.0f);
    DataRetrieval checkIndex = new DataRetrieval("students", List.of("*"), checkCondition, true);
    List<Row> indexResults = storageManager.readBlock(checkIndex);
    assertEquals(0, indexResults.size(), "Index lookup gpa<3.0 harus kosong setelah delete");

    // 4. Cek boundary gpa=3.0 (boundary tidak terpengaruh delete)
    WhereConditionNode boundaryCondition = buildComparison("gpa", "=", 3.0f);
    DataRetrieval checkBoundary = new DataRetrieval("students", List.of("*"), boundaryCondition, true);
    List<Row> boundaryResults = storageManager.readBlock(checkBoundary);
    assertEquals(1, boundaryResults.size(), "gpa=3.0f (ID 50) harus tetap ada");

    System.out.println("Delete with Range Index PASSED.");
  }

  @Test
  @DisplayName("Test: Update Kolom Terindeks (gpa) dengan Range (Index harus Diperbarui)")
  void testUpdateIndexedColumnWithRange() {
    System.out.println("--- testUpdateIndexedColumnWithRange ---");

    // Data: 3.1, 3.2, 3.8
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 1, "name", "A", "gpa", 3.1f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 2, "name", "B", "gpa", 3.2f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 3, "name", "C", "gpa", 3.8f)), null));

    // 1. Cek Awal (Range Index)
    WhereConditionNode checkAwalCondition = buildComparison("gpa", "<", 3.5f);
    DataRetrieval rangeCheckAwal = new DataRetrieval("students", List.of("id"), checkAwalCondition, true);
    List<Row> awalResults = storageManager.readBlock(rangeCheckAwal);
    assertEquals(2, awalResults.size(), "Awalnya, gpa<3.5 harus ada 2 rows (3.1, 3.2)");

    // 2. Update: Ubah gpa=3.1f (ID 1) menjadi 3.6f (Keluar dari range < 3.5)
    WhereConditionNode updateCondition = buildComparison("id", "=", 1);
    DataUpdate updateReq = new DataUpdate(
        "students",
        new Row(Map.of("gpa", 3.6f)),
        updateCondition);
    int updated = storageManager.updateBlock(updateReq);
    assertEquals(1, updated, "Harus mengupdate 1 baris");

    // 3. Verifikasi Index (Range Scan Check 1: ID 1 harus hilang)
    WhereConditionNode checkAkhir1Condition = buildComparison("gpa", "<", 3.5f);
    DataRetrieval rangeCheckAkhir1 = new DataRetrieval("students", List.of("id"), checkAkhir1Condition, true);
    List<Row> akhirResults1 = storageManager.readBlock(rangeCheckAkhir1);
    assertEquals(1, akhirResults1.size(), "Setelah update, gpa<3.5 hanya harus sisa 1 row (ID 2)");
    assertTrue(akhirResults1.stream().anyMatch(r -> (int) r.data().get("id") == 2));

    // 4. Verifikasi Index (Range Scan Check 2: ID 1 harus pindah ke range > 3.5)
    WhereConditionNode checkAkhir2Condition = buildComparison("gpa", ">=", 3.5f);
    DataRetrieval rangeCheckAkhir2 = new DataRetrieval("students", List.of("id"), checkAkhir2Condition, true);
    List<Row> akhirResults2 = storageManager.readBlock(rangeCheckAkhir2);
    // ID 1 (3.6f) + ID 3 (3.8f) = 2
    assertEquals(2, akhirResults2.size(), "Range search gpa>=3.5 harus menemukan 2 baris (ID 1, ID 3)");

    System.out.println("Update Indexed Column with Range PASSED.");
  }

  @Test
  @DisplayName("Test: Hash Index harus menolak Range Search (> / <)")
  void testHashIndexRejectsRangeSearch() {
    System.out.println("--- testHashIndexRejectsRangeSearch ---");

    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 10, "name", "A", "gpa", 3.0f)), null));
    storageManager.writeBlock(new DataWrite("students", new Row(Map.of("id", 20, "name", "B", "gpa", 3.0f)), null));

    WhereConditionNode rangeCondition = buildComparison("id", ">", 15);
    DataRetrieval rangeLookup = new DataRetrieval(
        "students",
        List.of("*"),
        rangeCondition,
        true);

    List<Row> results = storageManager.readBlock(rangeLookup);

    assertEquals(1, results.size(), "Range search id>15 (ID 20) harus menemukan 1 baris via Full Scan.");
    assertEquals(20, results.get(0).data().get("id"));

    System.out.println("Hash Index range lookup correctly fell back to Full Scan and PASSED.");

    // Note: Direct HashIndex Range Exception test commented out due to missing internal StorageManager getter methods
    // assertThrows(UnsupportedOperationException.class, () -> {
    //   Schema schema = storageManager.getCatalogManager().getSchema("students");
    //   IndexSchema hashIdx = schema.indexes().stream()
    //       .filter(i -> i.columnName().equals("id"))
    //       .findFirst().orElseThrow();
    //
    //   IIndex<?, ?> index = storageManager.getIndexManager().get(
    //       "students", hashIdx.columnName(), hashIdx.indexType().toString());
    //
    //   ((HashIndex) index).getAddresses(10, true, 30, true);
    // }, "HashIndex harus melempar UnsupportedOperationException untuk Range Scan.");
    //
    // System.out.println("Direct HashIndex Range Exception Test PASSED.");
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

    WhereConditionNode hashLookupCondition = buildComparison("name", "=", "Alice");
    DataRetrieval lookup = new DataRetrieval(
        "students",
        List.of("*"),
        hashLookupCondition,
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
        results.stream().anyMatch(r -> r.data().get("id").equals(1)
            && r.data().get("name").equals(longName1)));
    assertTrue(
        results.stream().anyMatch(r -> r.data().get("id").equals(2)
            && r.data().get("name").equals(longName2)));
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
        .writeBlock(new DataWrite("students",
            new Row(Map.of("id", 1, "name", "Ani", "gpa", 4.0f)), null));
    storageManager
        .writeBlock(new DataWrite("students",
            new Row(Map.of("id", 2, "name", "Budi", "gpa", 3.5f)), null));
    storageManager
        .writeBlock(new DataWrite("students",
            new Row(Map.of("id", 3, "name", "Ani", "gpa", 3.8f)), null)); // Nama
                                                                          // 'Ani'
                                                                          // duplikat

    // Tulis 2 baris ke 'courses'
    storageManager.writeBlock(
        new DataWrite("courses", new Row(Map.of("course_id", "IF3140", "credits", 3)), null));
    storageManager.writeBlock(
        new DataWrite("courses", new Row(Map.of("course_id", "IF3110", "credits", 4)), null));

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

    int deleted = storageManager.deleteBlock(new DataDeletion("students", buildComparison("id", "=", 1)));
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

    int deleted = storageManager.deleteBlock(new DataDeletion("students", buildComparison("id", "=", 20)));
    assertEquals(1, deleted);

    List<Row> fullScan = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
    assertEquals(2, fullScan.size());
    assertFalse(fullScan.stream().anyMatch(r -> r.data().get("id").equals(20)));

    List<Row> indexScan = storageManager
        .readBlock(new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 20), true));
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
      List<Row> credit3 = sm2
          .readBlock(new DataRetrieval("courses", List.of("*"), buildComparison("credits", "=", 3), true));
      assertEquals(2, credit3.size());
      assertTrue(credit3.stream().allMatch(r -> r.data().get("credits").equals(3)));

      List<Row> credit4 = sm2
          .readBlock(new DataRetrieval("courses", List.of("*"), buildComparison("credits", "=", 4), true));
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
    DataRetrieval query = new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 100), false);
    List<Row> results = storageManager.readBlock(query);

    assertEquals(2, results.size(), "Harus ada 2 baris dengan id=100");
    assertTrue(results.stream().allMatch(r -> r.data().get("id").equals(100)));
    assertTrue(results.stream().anyMatch(r -> r.data().get("name").equals("Alice")));
    assertTrue(results.stream().anyMatch(r -> r.data().get("name").equals("Charlie")));

    // Cari id=200 (single)
    query = new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 200), false);
    results = storageManager.readBlock(query);

    assertEquals(1, results.size(), "Harus ada 1 baris dengan id=200");
    assertEquals("Bob", results.get(0).data().get("name"));

    // Cari id=999 (tidak ada)
    query = new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 999), false);
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
    DataRetrieval query = new DataRetrieval("students", List.of("*"), buildComparison("name", "=", "Alice"), false);
    List<Row> results = storageManager.readBlock(query);

    assertEquals(2, results.size(), "Harus ada 2 baris dengan name='Alice'");
    assertTrue(results.stream().allMatch(r -> r.data().get("name").equals("Alice")));

    // Cari name='Bob'
    query = new DataRetrieval("students", List.of("*"), buildComparison("name", "=", "Bob"), false);
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
    DataRetrieval query = new DataRetrieval("students", List.of("*"), buildComparison("gpa", "=", 3.5f), false);
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
    DataRetrieval query = new DataRetrieval("students", List.of("*"), buildBinary(buildComparison("name", "=", "Alice"), "AND", buildComparison("gpa", "=", 3.5)), false);
    List<Row> results = storageManager.readBlock(query);

    assertEquals(1, results.size(), "Hanya 1 baris match name=Alice AND gpa=3.5");
    assertEquals(10, results.get(0).data().get("id"));

    // Query: name=Bob AND gpa=2.9
    query = new DataRetrieval("students", List.of("*"), buildBinary(buildComparison("name", "=", "Bob"), "AND", buildComparison("gpa", "=", 2.9f)), false);
    results = storageManager.readBlock(query);

    assertEquals(1, results.size(), "Hanya 1 baris match name=Bob AND gpa=2.9");
    assertEquals(40, results.get(0).data().get("id"));

    // Query: id=99 AND name=Alice (tidak ada match)
    query = new DataRetrieval("students", List.of("*"), buildBinary(buildComparison("id", "=", 99), "AND", buildComparison("name", "=", "Alice")), false);
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
    DataRetrieval query = new DataRetrieval("students", List.of("id", "name"), buildComparison("name", "=", "Alice"), false);
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
      String name = (i % 3 == 0) ? "AliceMediumName"
          : (i % 3 == 1) ? "BobMediumName" : "CharlieMediumName";
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
    List<Row> aliceRows = storageManager
        .readBlock(new DataRetrieval("students", List.of("*"), buildComparison("name", "=", "AliceMediumName"), false));
    assertEquals(100, aliceRows.size(), "Ada 100 baris dengan name=AliceMediumName (300/3)");

    // Filter 2
    List<Row> gpaBetween = storageManager
        .readBlock(new DataRetrieval("students", List.of("id", "gpa"), buildComparison("name", "=", "BobMediumName"),
            false));
    assertEquals(100, gpaBetween.size(), "Ada 100 baris dengan name=BobMediumName");
  }

  // @Test
  // @DisplayName("Test: GetSchema untuk tabel yang ada")
  // void testGetSchema() {
  // Schema schema = storageManager.getSchema("students");
  // assertNotNull(schema, "Schema untuk 'students' harus ada");
  // assertEquals("students", schema.tableName());
  // assertEquals("students.dat", schema.dataFile());
  // assertEquals(3, schema.columns().size(), "students punya 3 kolom");
  //
  // Schema notFound = storageManager.getSchema("nonexistent");
  // assertNull(notFound, "Schema untuk tabel non-eksisten harus null");
  // }

  @Test
  @DisplayName("Test: Concurrent Read & Write (Basic)")
  void testConcurrentReadWrite() throws InterruptedException {
    System.out.println("--- testConcurrentReadWrite ---");

    // Thread 1: Write 50 rows
    Thread writerThread = new Thread(() -> {
      for (int i = 0; i < 50; i++) {
        Row row = new Row(Map.of("id", 1000 + i, "name", "Writer" + i, "gpa",
            3.0f + (i % 10) * 0.1f));
        storageManager.writeBlock(new DataWrite("students", row, null));
      }
    });

    // Thread 2: Read rows (after initial write)
    Thread readerThread = new Thread(() -> {
      try {
        Thread.sleep(50); // Wait untuk writer memulai
        for (int i = 0; i < 5; i++) {
          List<Row> rows = storageManager.readBlock(
              new DataRetrieval("students", List.of("*"), null, false));
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

    List<Row> finalRows = storageManager
        .readBlock(new DataRetrieval("students", List.of("*"), null, false));
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
    int deleted = storageManager.deleteBlock(new DataDeletion("students", buildComparison("id", "=", 0)));
    assertEquals(4, deleted, "Harus delete 4 baris dengan id=0 (index 0, 3, 6, 9)");

    // Verifikasi
    List<Row> remaining = storageManager
        .readBlock(new DataRetrieval("students", List.of("*"), null, false));
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

    // empty filter (also null)
    results = storageManager.readBlock(new DataRetrieval("students", List.of("*"), null, false));
    assertEquals(3, results.size(), "empty filter harus return semua baris");
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
      storageManager.deleteBlock(new DataDeletion("students", buildComparison("id", "=", i)));
    }

    // Re-compute stats
    Map<String, Statistic> stats2 = storageManager.getAllStats();
    assertEquals(50, stats2.get("students").nr(), "After deleting 50, nr should be 50");
  }

  @Test
  @DisplayName("Test: updateBlock melakukan in-place update")
  void testUpdateBlockInPlace() {
    // Insert data awal
    storageManager.writeBlock(new DataWrite("students",
        new Row(Map.of("id", 1, "name", "Alice", "gpa", 3.0f)), null));

    // Update (ukuran sama/kecil) → harus in-place
    int updated = storageManager
        .updateBlock(new DataUpdate("students", new Row(Map.of("name", "Alice Updated")), // Hanya
                                                                                          // update
                                                                                          // name
            buildComparison("id", "=", 1)));

    assertEquals(1, updated);

    // Verifikasi
    List<Row> rows = storageManager.readBlock(
        new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 1), false));
    assertEquals(1, rows.size());
    assertEquals("Alice Updated", rows.get(0).data().get("name"));
    assertEquals(3.0f, rows.get(0).data().get("gpa")); // GPA tidak berubah
  }

  @Test
  @DisplayName("Test: updateBlock dengan ukuran record lebih besar (fallback ke delete+insert)")
  void testUpdateBlockWithLargerSize() {
    // Insert data dengan string pendek
    storageManager.writeBlock(new DataWrite("students",
        new Row(Map.of("id", 2, "name", "Bob", "gpa", 2.5f)), null));

    // Update dengan string yang sangat panjang (lebih besar dari ukuran awal)
    String longName = "A".repeat(200); // String panjang
    int updated = storageManager.updateBlock(new DataUpdate("students",
        new Row(Map.of("name", longName)), buildComparison("id", "=", 2)));

    assertEquals(1, updated);

    // Verifikasi data ter-update
    List<Row> rows = storageManager.readBlock(
        new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 2), false));
    assertEquals(1, rows.size());
    assertEquals(longName, rows.get(0).data().get("name"));
  }

  @Test
  @DisplayName("Test: updateBlock dengan multiple rows (batch update)")
  void testUpdateBlockMultipleRows() {
    // Insert beberapa data
    storageManager.writeBlock(new DataWrite("students",
        new Row(Map.of("id", 3, "name", "Charlie", "gpa", 3.0f)), null));
    storageManager.writeBlock(new DataWrite("students",
        new Row(Map.of("id", 4, "name", "David", "gpa", 3.0f)), null));
    storageManager.writeBlock(new DataWrite("students",
        new Row(Map.of("id", 5, "name", "Eve", "gpa", 3.0f)), null));

    // Update semua yang gpa=3.0f
    int updated = storageManager.updateBlock(new DataUpdate("students",
        new Row(Map.of("gpa", 3.5f)), buildComparison("gpa", "=", 3.0f)));

    assertEquals(3, updated, "Harus update 3 rows");

    // Verifikasi semua ter-update
    List<Row> rows = storageManager.readBlock(
        new DataRetrieval("students", List.of("*"), null, false));
    long countUpdated = rows.stream()
        .filter(r -> r.data().get("gpa").equals(3.5f))
        .count();
    assertEquals(3, countUpdated);
  }

  @Test
  @DisplayName("Test: updateBlock update indexed column (index harus ter-update)")
  void testUpdateBlockWithIndexedColumn() {
    // Insert dengan id=10 (id punya index Hash)
    storageManager.writeBlock(new DataWrite("students",
        new Row(Map.of("id", 10, "name", "Frank", "gpa", 3.0f)), null));

    // Update id (kolom yang di-index) - ini harus update index juga
    int updated = storageManager.updateBlock(new DataUpdate("students",
        new Row(Map.of("id", 11)), buildComparison("id", "=", 10)));

    assertEquals(1, updated);

    // Verifikasi dengan index lookup
    List<Row> oldLookup = storageManager.readBlock(
        new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 10), true));
    assertEquals(0, oldLookup.size(), "Index lookup id=10 harus kosong");

    List<Row> newLookup = storageManager.readBlock(
        new DataRetrieval("students", List.of("*"), buildComparison("id", "=", 11), true));
    assertEquals(1, newLookup.size(), "Index lookup id=11 harus ada 1 row");
  }

  @Test
  @DisplayName("Test: GetSchema untuk tabel yang ada (Memverifikasi Metadata Lengkap)")
  void testGetSchema() {

    // --- DEBUG: Menampilkan Detail Skema 'students' (Tabel 1) ---
    System.out.println("\n--- DEBUG: Menampilkan Detail Skema 'students' ---");
    Schema schema = storageManager.getSchema("students");

    // Verifikasi atribut dasar students
    assertNotNull(schema, "Schema untuk 'students' harus ada");
    assertEquals("students", schema.tableName());

    // OUTPUT DETAIL 'students'
    System.out.printf("Nama Tabel: %s%n", schema.tableName());
    System.out.printf("File Data: %s%n", schema.dataFile());

    System.out.printf("%nKolom (%d):%n", schema.columns().size());
    for (Column col : schema.columns()) {
      System.out.printf("  - %s (%s, Panjang: %d)%n", col.name(), col.type(), col.length());
    }

    System.out.printf("%nIndeks (%d):%n", schema.indexes().size());
    for (IndexSchema idx : schema.indexes()) {
      System.out.printf("  - Nama: %s, Kolom: %s, Tipe: %s, File: %s%n",
          idx.indexName(), idx.columnName(), idx.indexType(), idx.indexFile());
    }

    assertTrue(schema.getForeignKeys().isEmpty(), "students tidak boleh punya Foreign Key");
    System.out.printf("%nForeign Keys (%d):%n", schema.getForeignKeys().size());
    System.out.println("  (Tidak ada Foreign Key)");

    // --- Verifikasi skema 'courses' (Tabel 2) ---
    System.out.println("\n--- DEBUG: Menampilkan Detail Skema 'courses' ---");
    Schema coursesSchema = storageManager.getSchema("courses");
    assertNotNull(coursesSchema, "Schema untuk 'courses' harus ada");
    assertTrue(coursesSchema.indexes().isEmpty(), "courses tidak punya indeks");
    assertTrue(coursesSchema.getForeignKeys().isEmpty(), "courses tidak punya Foreign Key");

    // OUTPUT DETAIL 'courses'
    System.out.printf("Nama Tabel: %s%n", coursesSchema.tableName());
    System.out.printf("File Data: %s%n", coursesSchema.dataFile());

    System.out.printf("%nKolom (%d):%n", coursesSchema.columns().size());
    for (Column col : coursesSchema.columns()) {
      System.out.printf("  - %s (%s, Panjang: %d)%n", col.name(), col.type(), col.length());
    }

    System.out.printf("%nIndeks (%d):%n", coursesSchema.indexes().size());
    if (coursesSchema.indexes().isEmpty()) {
      System.out.println("  (Tidak ada Indeks)");
    } else {
      for (IndexSchema idx : coursesSchema.indexes()) {
        System.out.printf("  - Nama: %s, Kolom: %s, Tipe: %s, File: %s%n",
            idx.indexName(), idx.columnName(), idx.indexType(), idx.indexFile());
      }
    }

    assertTrue(coursesSchema.getForeignKeys().isEmpty(), "courses tidak punya Foreign Key");
    System.out.printf("%nForeign Keys (%d):%n", coursesSchema.getForeignKeys().size());
    System.out.println("  (Tidak ada Foreign Key)");

    // --- Verifikasi skema 'enrollments' (Tabel 3 - dengan FK) ---
    System.out.println("\n--- DEBUG: Menampilkan Detail Skema 'enrollments' ---");
    Schema enrollmentsSchema = storageManager.getSchema("enrollments");
    assertNotNull(enrollmentsSchema, "Schema untuk 'enrollments' harus ada");

    // Verifikasi FK
    assertEquals(1, enrollmentsSchema.getForeignKeys().size(), "enrollments harus punya 1 Foreign Key");

    // OUTPUT DETAIL 'enrollments'
    System.out.printf("Nama Tabel: %s%n", enrollmentsSchema.tableName());
    System.out.printf("File Data: %s%n", enrollmentsSchema.dataFile());

    System.out.printf("%nKolom (%d):%n", enrollmentsSchema.columns().size());
    for (Column col : enrollmentsSchema.columns()) {
      System.out.printf("  - %s (%s, Panjang: %d)%n", col.name(), col.type(), col.length());
    }

    System.out.printf("%nIndeks (%d):%n", enrollmentsSchema.indexes().size());
    for (IndexSchema idx : enrollmentsSchema.indexes()) {
      System.out.printf("  - Nama: %s, Kolom: %s, Tipe: %s, File: %s%n",
          idx.indexName(), idx.columnName(), idx.indexType(), idx.indexFile());
    }

    System.out.printf("%nForeign Keys (%d):%n", enrollmentsSchema.getForeignKeys().size());

    // FIX: Menggunakan Foreign Key Schema (record) yang benar
    for (ForeignKeySchema fk : enrollmentsSchema.getForeignKeys()) {
      // Mengakses field record secara langsung: constraintName, columnName,
      // referenceTable, referenceColumn
      System.out.printf("  - Nama: %s, Kolom Lokal: %s, Referensi: %s(%s), CASCADE: %b%n",
          fk.constraintName(), fk.columnName(), fk.referenceTable(), fk.referenceColumn(), fk.isCascading());
    }
  }

  @Test
  @DisplayName("Test: getDependentTables correctly identifies referencing tables")
  void testGetDependentTables() {
    System.out.println("--- testGetDependentTables ---");

    List<String> dependents = storageManager.getDependentTables("students");
    assertNotNull(dependents);
    assertEquals(1, dependents.size(), "students harus direferensikan oleh 1 tabel (enrollments)");
    assertEquals("enrollments", dependents.get(0));

    List<String> enrollDependents = storageManager.getDependentTables("enrollments");
    assertTrue(enrollDependents.isEmpty(), "enrollments tidak boleh punya dependent tables");

    List<String> courseDependents = storageManager.getDependentTables("courses");
    assertTrue(courseDependents.isEmpty(), "courses tidak boleh punya dependent tables");

    System.out.println("getDependentTables PASSED.");
  }

  @Test
  @DisplayName("Test: DROP TABLE RESTRICT fails when dependencies exist")
  void testDropTableRestrict() {
    System.out.println("--- testDropTableRestrict ---");

    Exception exception = assertThrows(RuntimeException.class, () -> {
      storageManager.dropTable("students", "RESTRICT");
    });

    String expectedMessage = "referenced by [enrollments]";
    String actualMessage = exception.getMessage();

    assertTrue(actualMessage.contains(expectedMessage),
        "Pesan error harus menyebutkan tabel yang mereferensi (enrollments). Got: " + actualMessage);

    // Verifikasi tabel 'students' masih ada (tidak terhapus)
    assertNotNull(storageManager.getSchema("students"), "Tabel students harus tetap ada setelah gagal drop");

    System.out.println("DROP TABLE RESTRICT functionality PASSED.");
  }

  @Test
  @DisplayName("Test: DROP TABLE CASCADE removes table and cleans up Child FK")
  void testDropTableCascade() {
    System.out.println("--- testDropTableCascade ---");

    Schema enrollSchemaBefore = storageManager.getSchema("enrollments");
    boolean hasFkBefore = enrollSchemaBefore.getForeignKeys().stream()
        .anyMatch(fk -> fk.referenceTable().equals("students"));
    assertTrue(hasFkBefore, "Awalnya enrollments harus punya FK ke students");

    int result = storageManager.dropTable("students", "CASCADE");
    assertEquals(0, result, "dropTable harus return 0 (sukses)");

    assertNull(storageManager.getSchema("students"), "Schema students harus null (terhapus)");

    File studentFile = new File(TEST_DIR, "students.dat");
    assertFalse(studentFile.exists(), "File students.dat harus terhapus");

    Schema enrollSchemaAfter = storageManager.getSchema("enrollments");
    assertNotNull(enrollSchemaAfter, "Tabel enrollments (anak) TIDAK boleh terhapus");

    boolean hasFkAfter = enrollSchemaAfter.getForeignKeys().stream()
        .anyMatch(fk -> fk.referenceTable().equals("students"));
    assertFalse(hasFkAfter, "Foreign Key ke 'students' harus dihapus dari skema enrollments");

    System.out.println("DROP TABLE CASCADE functionality PASSED.");
  }

}
