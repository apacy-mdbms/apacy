package com.apacy.queryprocessor;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.*;
import com.apacy.queryprocessor.mocks.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class QueryProcessorTest {
    
    private QueryProcessor queryProcessor;
    
    @BeforeEach
    void setUp() {
        // Menggunakan Mock yang sudah diperbarui logic-nya
        IQueryOptimizer mockQO = new MockQueryOptimizer();
        IStorageManager mockSM = new MockStorageManager();
        IConcurrencyControlManager mockCCM = new MockConcurrencyControlManager();
        IFailureRecoveryManager mockFRM = new MockFailureRecoveryManager();

        queryProcessor = new QueryProcessor(mockQO, mockSM, mockCCM, mockFRM);
    }
    
    @Test
    void testComponentName() {
        assertEquals("Query Processor", queryProcessor.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> queryProcessor.initialize());
    }
    
    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> queryProcessor.shutdown());
    }

    @Test
    void test_executeQuery_SELECT_Success(){
        // MockQueryOptimizer akan mengenali kata "SELECT"
        ExecutionResult result = queryProcessor.executeQuery("SELECT * FROM users");

        assertNotNull(result);
        assertTrue(result.success(), "SELECT harus sukses");

        assertEquals("SELECT executed successfully", result.message());
        assertEquals("SELECT", result.operation());
        
        // MockStorageManager mengembalikan 5 baris dummy untuk tabel 'users'
        assertEquals(5, result.affectedRows()); 
        assertNotNull(result.rows());
        assertEquals(5, result.rows().size());
        
        // Verifikasi data baris pertama
        assertEquals("Naufarrel", result.rows().get(0).get("name"));
    }

    @Test
    void test_executeQuery_INSERT_Success(){
        // MockQueryOptimizer mengenali "INSERT"
        // MockStorageManager.writeBlock mengembalikan 1
        ExecutionResult result = queryProcessor.executeQuery("INSERT INTO users (name) VALUES ('Budi')");

        assertNotNull(result);
        // Perhatikan: Implementasi default QP mungkin belum handle INSERT di switch-case 'executeQuery'
        // Jika belum diimplementasi di QueryProcessor.java, ini mungkin fail atau throw Exception.
        // Namun, ini adalah tes yang diharapkan LULUS setelah Anda mengupdate QueryProcessor.
        
        // Asumsi: QP sudah diupdate untuk handle INSERT (jika belum, tes ini mengingatkan untuk update QP)
        assertTrue(result.success(), "INSERT harus sukses");
        assertEquals("INSERT executed successfully", result.message());
        assertEquals(1, result.affectedRows()); 
    }

    @Test
    void test_executeQuery_UPDATE_Success(){
        // MockQueryOptimizer mengenali "UPDATE"
        // MockStorageManager.writeBlock mengembalikan 1
        ExecutionResult result = queryProcessor.executeQuery("UPDATE users SET salary = 60000 WHERE id = 1");

        assertNotNull(result);
        assertTrue(result.success(), "UPDATE harus sukses");
        assertEquals("UPDATE executed successfully", result.message());
        assertEquals(1, result.affectedRows());
    }

    @Test
    void test_executeQuery_DELETE_Success(){
        // MockQueryOptimizer mengenali "DELETE"
        // MockStorageManager.deleteBlock mengembalikan 1
        ExecutionResult result = queryProcessor.executeQuery("DELETE FROM users WHERE id = 5");

        assertNotNull(result);
        assertTrue(result.success(), "DELETE harus sukses");
        assertEquals("DELETE executed successfully", result.message());
        assertEquals(1, result.affectedRows());
    }

    @Test
    void test_executeQuery_Fail_UnknownQuery(){
        // Query ngawur yang tidak dikenali MockQO
        ExecutionResult result = queryProcessor.executeQuery("JOGET DULU GAK SIE");

        assertNotNull(result);
        assertFalse(result.success(), "Query invalid harus gagal");
        // Pesan error tergantung implementasi di QP (misal: NPE atau Unsupported)
        assertNotNull(result.message()); 
    }
}