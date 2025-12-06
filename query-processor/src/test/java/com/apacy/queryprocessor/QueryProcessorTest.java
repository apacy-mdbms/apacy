package com.apacy.queryprocessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.plan.ModifyNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.enums.DataType;
import com.apacy.queryprocessor.mocks.MockConcurrencyControlManager;
import com.apacy.queryprocessor.mocks.MockFailureRecoveryManager;
import com.apacy.queryprocessor.mocks.MockQueryOptimizer;
import com.apacy.queryprocessor.mocks.MockStorageManager;

class QueryProcessorTest {
    
    private QueryProcessor queryProcessor;
    private MockQueryOptimizer mockQO;
    private MockStorageManager mockSM;
    private MockConcurrencyControlManager mockCCM;
    private MockFailureRecoveryManager mockFRM;
    
    @BeforeEach
    void setUp() {
        // Inisialisasi Mock Components
        mockQO = new MockQueryOptimizer();
        mockSM = new MockStorageManager();
        mockCCM = new MockConcurrencyControlManager();
        mockFRM = new MockFailureRecoveryManager();

        // --- Setup Schema agar QueryBinder tidak error ---
        
        // Schema untuk tabel 'users'
        Schema usersSchema = new Schema(
            "users", 
            "users.dat", 
            List.of(
                new Column("id", DataType.INTEGER),
                new Column("name", DataType.VARCHAR, 100)
            ), 
            Collections.emptyList()
        );
        mockSM.addSchema("users", usersSchema);

        // Schema untuk tabel 'test_table'
        Schema testTableSchema = new Schema(
            "test_table", 
            "test_table.dat", 
            List.of(
                new Column("id", DataType.INTEGER),
                new Column("val", DataType.VARCHAR)
            ), 
            Collections.emptyList()
        );
        mockSM.addSchema("test_table", testTableSchema);
        // ------------------------------------------------------

        // Inisialisasi Query Processor dengan Mock
        queryProcessor = new QueryProcessor(mockQO, mockSM, mockCCM, mockFRM);
        try {
            queryProcessor.initialize();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    void testComponentName() {
        assertEquals("Query Processor", queryProcessor.getComponentName());
    }
    
    @Test
    void test_executeQuery_SELECT_Success() {
        // 1. Stubbing: Siapkan data palsu di Storage Manager
        Row dummyRow = new Row(Map.of("name", "Naufarrel", "id", 1));
        mockSM.setRowsToReturn(List.of(dummyRow));

        // 2. Stubbing: Siapkan Plan Query di Optimizer
        ScanNode scanNode = new ScanNode("users", "u");
        
        ParsedQuery selectQuery = new ParsedQuery(
            "SELECT",
            scanNode, // Root plan
            List.of("users"),
            List.of("*"),
            Collections.emptyList(),
            null, null, null, false, true
        );
        mockQO.setParsedQueryToReturn(selectQuery);

        // 3. Eksekusi
        ExecutionResult result = queryProcessor.executeQuery("SELECT * FROM users;");

        // 4. Verifikasi Hasil
        if (!result.success()) {
            System.out.println("Test SELECT Failed with message: " + result.message());
        }

        assertNotNull(result);
        assertTrue(result.success(), "SELECT harus sukses. Msg: " + result.message());
        assertEquals(1, result.affectedRows(), "Harus mengembalikan 1 baris sesuai stub mockSM");
        
        // Ambil data (flexible check untuk handle aliasing jika ada)
        Object nameVal = result.rows().get(0).get("name");
        if (nameVal == null) nameVal = result.rows().get(0).get("u.name");
        assertEquals("Naufarrel", nameVal);

        // 5. Verifikasi Interaksi (Spying)
        assertEquals(1, mockSM.getReadBlockCallCount(), "sm.readBlock() harus dipanggil 1 kali");
        assertEquals(1, mockCCM.getBeginTransactionCallCount(), "Transaction harus dimulai");
        assertEquals(1, mockCCM.getEndTransactionCallCount(), "Transaction harus diakhiri (commit)");
        assertTrue(mockCCM.getLastEndCommitStatus(), "Status akhir transaksi harus COMMIT");
    }

    @Test
    void test_executeQuery_INSERT_Success() {
        // 1. Stubbing: SM mengembalikan 1 row affected
        mockSM.setWriteAffectedRowsToReturn(1);

        // 2. Stubbing: Optimizer mengembalikan tipe INSERT
        ParsedQuery insertQuery = new ParsedQuery(
            "INSERT",
            null, // PlanRoot null agar QP pakai fallback ModifyNode
            List.of("users"),
            List.of("name"),
            List.of("Budi"),
            null, null, null, false, true
        );
        mockQO.setParsedQueryToReturn(insertQuery);

        // 3. Eksekusi
        ExecutionResult result = queryProcessor.executeQuery("INSERT INTO users (name) VALUES ('Budi');");

        // 4. Verifikasi
        assertNotNull(result);
        assertTrue(result.success(), "INSERT harus sukses. Msg: " + result.message());
        assertEquals(1, result.affectedRows());
        
        // 5. Verifikasi Interaksi
        assertEquals(1, mockSM.getWriteBlockCallCount(), "sm.writeBlock() harus dipanggil");
        
        // Verifikasi Log: BEGIN dan COMMIT (karena Auto-Commit)
        // Mock FRM hanya menyimpan last event, jadi last event harus COMMIT
        assertEquals(2, mockFRM.getWriteTransactionLogCallCount(), "Harus ada 2 log transaksi (BEGIN & COMMIT)");
        assertEquals("COMMIT", mockFRM.getLastTransactionLogLifecycleEvent(), "Log terakhir harus COMMIT");
    }

    @Test
    void test_executeQuery_UPDATE_Atomic_Success() {
        // 1. Stubbing: Setup Data untuk dibaca saat UPDATE (Scan phase)
        // Update operator butuh iterasi child.next(). Kita kasih 1 row dummy.
        mockSM.setRowsToReturn(List.of(new Row(Map.of("id", 1, "name", "OldName"))));
        
        // 2. Stubbing: Hasil eksekusi updateBlock
        mockSM.setUpdateAffectedRowsToReturn(5);
        
        // 3. Stubbing: Plan Tree yang valid
        // Kita butuh ModifyNode -> Child: ScanNode agar ModifyOperator bisa iterasi row
        ScanNode scanNode = new ScanNode("users", "u");
        ModifyNode modifyNode = new ModifyNode("UPDATE", scanNode, "users", List.of("name"), List.of("John"));

        ParsedQuery updateQuery = new ParsedQuery(
            "UPDATE",
            modifyNode, // Set planRoot explicit
            List.of("users"),
            List.of("name"),
            List.of("John"),
            null, null, null, false, true
        );
        mockQO.setParsedQueryToReturn(updateQuery);

        // 4. Eksekusi
        ExecutionResult result = queryProcessor.executeQuery("UPDATE users SET name = 'John' WHERE id = 1;");
        
        // 5. Verifikasi
        assertNotNull(result);
        assertTrue(result.success(), "UPDATE harus sukses. Msg: " + result.message());
        assertEquals(5, result.affectedRows(), "Affected rows harus sesuai return dari mockSM.updateBlock"); 
        
        assertEquals(1, mockSM.getReadBlockCallCount(), "Harus ada scan (readBlock) sebelum update");
        assertEquals(1, mockSM.getUpdateBlockCallCount(), "sm.updateBlock() harus dipanggil");
    }

    @Test
    void test_executeQuery_Fail_InvalidQuery() {
        // 1. Stubbing: Optimizer mengembalikan NULL (simulasi query tidak valid)
        mockQO.setParsedQueryToReturn(null);

        // 2. Eksekusi
        ExecutionResult result = queryProcessor.executeQuery("JOGET DULU GAK SIE;");

        // 3. Verifikasi
        assertNotNull(result);
        assertFalse(result.success(), "Query invalid harus gagal");
        
        // 4. Verifikasi Recovery
        // Pastikan transaksi dibatalkan dan recovery dipanggil
        assertEquals(1, mockCCM.getEndTransactionCallCount(), "Transaksi harus ditutup");
        assertFalse(mockCCM.getLastEndCommitStatus(), "Transaksi harus di-ROLLBACK (false)");
        assertEquals(1, mockFRM.getRecoverCallCount(), "frm.recover() harus dipanggil");
        assertEquals("ABORT", mockFRM.getLastTransactionLogLifecycleEvent(), "Log ABORT harus dicatat");
    }

    @Test
    void test_TransactionLifecycle_Logging() {
        // Test khusus untuk memastikan urutan logging transaksi
        
        ScanNode scanNode = new ScanNode("test_table", "t");
        ParsedQuery query = new ParsedQuery("SELECT", scanNode, List.of("test_table"), List.of("*"), null, null, null, null, false, true);
        mockQO.setParsedQueryToReturn(query);

        queryProcessor.executeQuery("SELECT * FROM test_table");

        // Cek urutan log di FRM
        // Harapan: 1. BEGIN, 2. COMMIT (Total 2 panggilan)
        assertEquals(2, mockFRM.getWriteTransactionLogCallCount(), "Harus ada 2 log (BEGIN dan COMMIT)"); 
        assertEquals("COMMIT", mockFRM.getLastTransactionLogLifecycleEvent(), "Log terakhir harus COMMIT");
    }
}