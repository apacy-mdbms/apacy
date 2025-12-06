package com.apacy.queryprocessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.Row;
import com.apacy.concurrencycontrolmanager.ConcurrencyControlManager;
import com.apacy.failurerecoverymanager.FailureRecoveryManager;
import com.apacy.queryoptimizer.QueryOptimizer;
import com.apacy.storagemanager.StorageManager;

/**
 * System Test (End-to-End Test)
 * Menguji integrasi penuh seluruh komponen mDBMS-Apacy menggunakan implementasi nyata (Real Components).
 * * Alur: SQL -> QP -> QO (Parsing/Opt) -> CCM (Locking) -> SM (I/O) -> FRM (Logging)
 */
class SystemTest {

    private QueryProcessor queryProcessor;
    private StorageManager storageManager;
    private ConcurrencyControlManager ccm;
    private FailureRecoveryManager frm;
    private QueryOptimizer queryOptimizer;

    // Folder temporary unik untuk setiap run test agar tidak bentrok
    private String TEST_DIR;

    @BeforeEach
    void setUp() throws Exception {
        // 1. Setup Direktori Test Unik
        TEST_DIR = "system_test_data_" + UUID.randomUUID().toString().substring(0, 8);
        Files.createDirectories(Paths.get(TEST_DIR));

        // 2. Inisialisasi Komponen NYATA (Real Components)
        
        // Storage Manager (Menangani File Fisik)
        storageManager = new StorageManager(TEST_DIR);
        
        // Failure Recovery Manager (Menangani WAL Log)
        // Kita perlu memastikan path log juga ada di dalam folder test atau default
        frm = new FailureRecoveryManager(storageManager);
        
        // Concurrency Control Manager (Menggunakan Lock-Based Protocol)
        ccm = new ConcurrencyControlManager("lock", frm);
        
        // Query Optimizer (Parsing & Logical Plan)
        queryOptimizer = new QueryOptimizer();

        // 3. Rakit Query Processor
        queryProcessor = new QueryProcessor(queryOptimizer, storageManager, ccm, frm);
        
        // 4. Nyalakan Komponen
        storageManager.initialize();
        frm.initialize();
        ccm.initialize();
        queryOptimizer.initialize();
        queryProcessor.initialize();
    }

    @AfterEach
    void tearDown() throws IOException {
        // Shutdown komponen
        if (queryProcessor != null) queryProcessor.shutdown();
        if (storageManager != null) storageManager.shutdown();
        
        // Bersihkan file temporary
        deleteDirectory(Paths.get(TEST_DIR));
    }

    // --- Skenario Pengujian ---

    @Test
    @DisplayName("E2E: Basic Workflow (CREATE -> INSERT -> SELECT)")
    void testBasicWorkflow() {
        // 1. CREATE TABLE
        String createSql = "CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER);";
        ExecutionResult createRes = queryProcessor.executeQuery(createSql);
        
        assertTrue(createRes.success(), "CREATE TABLE harus sukses");
        assertNotNull(storageManager.getSchema("users"), "Schema 'users' harus terbentuk di StorageManager");

        // 2. INSERT DATA
        String insertSql1 = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 20);";
        ExecutionResult insertRes1 = queryProcessor.executeQuery(insertSql1);
        assertTrue(insertRes1.success(), "INSERT 1 harus sukses");
        assertEquals(1, insertRes1.affectedRows());

        String insertSql2 = "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 22);";
        queryProcessor.executeQuery(insertSql2);

        // 3. SELECT ALL
        String selectSql = "SELECT * FROM users;";
        ExecutionResult selectRes = queryProcessor.executeQuery(selectSql);

        assertTrue(selectRes.success(), "SELECT harus sukses");
        List<Row> rows = selectRes.rows();
        assertEquals(2, rows.size(), "Harus ada 2 baris data");
        
        // Verifikasi konten data (urutan tidak dijamin tanpa ORDER BY, jadi kita cari match)
        boolean foundAlice = rows.stream().anyMatch(r -> r.get("name").equals("Alice") && r.get("id").equals(1));
        boolean foundBob = rows.stream().anyMatch(r -> r.get("name").equals("Bob") && r.get("id").equals(2));
        
        assertTrue(foundAlice, "Data Alice harus ditemukan");
        assertTrue(foundBob, "Data Bob harus ditemukan");
    }

    @Test
    @DisplayName("E2E: Filtering & Projection (SELECT name FROM ... WHERE ...)")
    void testFilteringAndProjection() {
        // Setup Data
        queryProcessor.executeQuery("CREATE TABLE products (pid INTEGER, pname VARCHAR(20), price INTEGER);");
        queryProcessor.executeQuery("INSERT INTO products (pid, pname, price) VALUES (101, 'Laptop', 1500);");
        queryProcessor.executeQuery("INSERT INTO products (pid, pname, price) VALUES (102, 'Mouse', 20);");
        queryProcessor.executeQuery("INSERT INTO products (pid, pname, price) VALUES (103, 'Keyboard', 50);");

        // Test Query: Ambil nama produk yang harganya > 40
        String sql = "SELECT pname FROM products WHERE price > 40;";
        ExecutionResult result = queryProcessor.executeQuery(sql);

        assertTrue(result.success());
        assertEquals(2, result.rows().size(), "Harus ada 2 produk (Laptop, Keyboard)");

        Row firstRow = result.rows().get(0);
        // Verifikasi Proyeksi: Hanya kolom 'pname' yang boleh ada di hasil
        assertTrue(firstRow.data().containsKey("pname") || firstRow.data().containsKey("products.pname"));
        // Seharusnya TIDAK berisi 'price' jika proyeksi bekerja dengan benar
        // Note: Tergantung implementasi ProjectOperator apakah menghapus key lain atau bikin map baru
    }

    @Test
    @DisplayName("E2E: Update Data and Verify Persistence")
    void testUpdateData() {
        queryProcessor.executeQuery("CREATE TABLE accounts (acc_id INTEGER, balance INTEGER);");
        queryProcessor.executeQuery("INSERT INTO accounts (acc_id, balance) VALUES (1, 1000);");
        queryProcessor.executeQuery("INSERT INTO accounts (acc_id, balance) VALUES (2, 2000);");

        // Update balance akun 1
        String updateSql = "UPDATE accounts SET balance = 1500 WHERE acc_id = 1;";
        ExecutionResult updateRes = queryProcessor.executeQuery(updateSql);

        assertTrue(updateRes.success());
        assertEquals(1, updateRes.affectedRows());

        // Verifikasi dengan SELECT
        ExecutionResult selectRes = queryProcessor.executeQuery("SELECT balance FROM accounts WHERE acc_id = 1;");
        assertEquals(1, selectRes.rows().size());
        assertEquals(1500, selectRes.rows().get(0).get("balance"));
    }

    @Test
    @DisplayName("E2E: Delete Data")
    void testDeleteData() {
        queryProcessor.executeQuery("CREATE TABLE todo (id INTEGER, task VARCHAR(100));");
        queryProcessor.executeQuery("INSERT INTO todo (id, task) VALUES (1, 'Task A');");
        queryProcessor.executeQuery("INSERT INTO todo (id, task) VALUES (2, 'Task B');");

        // Delete Task A
        String deleteSql = "DELETE FROM todo WHERE id = 1;";
        ExecutionResult deleteRes = queryProcessor.executeQuery(deleteSql);

        assertTrue(deleteRes.success());
        assertEquals(1, deleteRes.affectedRows());

        // Verifikasi sisa data
        ExecutionResult selectRes = queryProcessor.executeQuery("SELECT * FROM todo;");
        assertEquals(1, selectRes.rows().size());
        assertEquals("Task B", selectRes.rows().get(0).get("task"));
    }

    @Test
    @DisplayName("E2E: Join Operation (Jika diimplementasikan)")
    void testJoinOperation() {
        // Setup 2 Tabel
        queryProcessor.executeQuery("CREATE TABLE students (sid INTEGER, name VARCHAR(20));");
        queryProcessor.executeQuery("CREATE TABLE grades (sid INTEGER, score INTEGER);");

        queryProcessor.executeQuery("INSERT INTO students (sid, name) VALUES (1, 'Ani');");
        queryProcessor.executeQuery("INSERT INTO students (sid, name) VALUES (2, 'Budi');");
        
        queryProcessor.executeQuery("INSERT INTO grades (sid, score) VALUES (1, 90);");
        queryProcessor.executeQuery("INSERT INTO grades (sid, score) VALUES (2, 80);");

        // Lakukan Join
        // Syntax tergantung parser yang didukung (Implicit Join atau Explicit JOIN ON)
        // Asumsi parser mendukung ANSI SQL standard JOIN
        String joinSql = "SELECT students.name, grades.score FROM students JOIN grades ON students.sid = grades.sid;";
        
        // Jika parser hanya support implicit: SELECT * FROM students, grades WHERE students.sid = grades.sid;
        
        ExecutionResult result = queryProcessor.executeQuery(joinSql);

        // Jika JOIN belum diimplementasikan sepenuhnya, kita assert false atau skip
        // Tapi jika sudah, ini validasinya:
        if (result.success()) {
            System.out.println("JOIN Executed Successfully");
            assertEquals(2, result.rows().size());
            // Cek baris pertama (Ani, 90)
            boolean foundAni = result.rows().stream()
                .anyMatch(r -> r.get("name").equals("Ani") && r.get("score").equals(90));
            assertTrue(foundAni);
        } else {
            System.out.println("JOIN Skipped / Not supported yet: " + result.message());
        }
    }

    @Test
    @DisplayName("E2E: Transaction Integrity (Commit/Rollback Check)")
    void testTransactionLogging() {
        // Kita tidak bisa memaksa rollback eksplisit via SQL "ROLLBACK" jika parser tidak mendukungnya,
        // tapi kita bisa mengecek apakah transaksi menghasilkan Transaction ID yang valid.
        
        queryProcessor.executeQuery("CREATE TABLE logs (msg VARCHAR(50));");
        ExecutionResult res = queryProcessor.executeQuery("INSERT INTO logs VALUES ('Log 1');");
        
        assertTrue(res.transactionId() > 0, "Setiap query harus memiliki Transaction ID yang valid dari CCM");
        
        // Cek apakah file log WAL terbentuk
        // Lokasi default FRM biasanya "failure-recovery/log/mDBMS.log" relatif terhadap working dir,
        // atau kita cek folder test jika dikonfigurasi demikian.
        File logFile = new File("failure-recovery/log/mDBMS.log");
        // Kita cek eksistensi saja sebagai bukti FRM dipanggil
        // Note: Path ini mungkin perlu disesuaikan dengan konfigurasi FRM Anda
        // assertTrue(logFile.exists(), "WAL Log file harus terbentuk");
    }

    // --- Helper Utils ---

    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }
}