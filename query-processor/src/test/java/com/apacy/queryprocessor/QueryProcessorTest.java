package com.apacy.queryprocessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.queryprocessor.mocks.MockConcurrencyControlManager;
import com.apacy.queryprocessor.mocks.MockFailureRecoveryManager;
import com.apacy.queryprocessor.mocks.MockQueryOptimizer;
import com.apacy.queryprocessor.mocks.MockStorageManager;

class QueryProcessorTest {
    
    private QueryProcessor queryProcessor;
    private MockQueryOptimizer mockQO;
    private MockStorageManager mockSM;
    
    @BeforeEach
    void setUp() {
        mockQO = new MockQueryOptimizer();
        mockSM = new MockStorageManager();
        IConcurrencyControlManager mockCCM = new MockConcurrencyControlManager();
        IFailureRecoveryManager mockFRM = new MockFailureRecoveryManager();

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
        ExecutionResult result = queryProcessor.executeQuery("SELECT * FROM users");

        System.out.println(result.rows());
        assertNotNull(result);
        assertTrue(result.success(), "SELECT harus sukses");
        assertEquals("SELECT executed successfully", result.message());
        
        assertEquals(5, result.affectedRows());
        
        Object nameVal = result.rows().get(0).get("name");
        if (nameVal == null) nameVal = result.rows().get(0).get("u.name");
        
        assertEquals("Naufarrel", nameVal);
    }

    @Test
    void test_executeQuery_INSERT_Success() {
        ExecutionResult result = queryProcessor.executeQuery("INSERT INTO users (name) VALUES ('Budi')");

        assertNotNull(result);
        assertTrue(result.success(), "INSERT harus sukses");
        assertEquals("INSERT executed successfully", result.message());
        assertEquals(1, result.affectedRows());
    }

    @Test
    void test_executeQuery_UPDATE_Atomic_Success() {
        ExecutionResult result = queryProcessor.executeQuery("UPDATE users SET name = 'John' WHERE id = 1");
        
        assertNotNull(result);
        assertTrue(result.success(), "UPDATE harus sukses");
        assertEquals(5, result.affectedRows()); 
    }

    @Test
    void test_executeQuery_Fail_UnknownQuery() {
        ExecutionResult result = queryProcessor.executeQuery("JOGET DULU GAK SIE");

        assertNotNull(result);
        assertFalse(result.success(), "Query invalid harus gagal");
        assertNotNull(result.message()); 
    }
}