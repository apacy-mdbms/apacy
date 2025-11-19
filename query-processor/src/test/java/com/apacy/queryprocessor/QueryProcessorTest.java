package com.apacy.queryprocessor;

import com.apacy.common.DBMSComponent;
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
        IQueryOptimizer mockQO = new MockQueryOptimizer();
        IStorageManager mockSM = new MockStorageManager();
        IConcurrencyControlManager mockCCM = new MockConcurrencyControlManager();
        IFailureRecoveryManager mockFRM = new MockFailureRecoveryManager();

        queryProcessor = new QueryProcessor(mockQO, mockSM, mockCCM, mockFRM);
    }
    
    @Test
    void testComponentName() {
        assertEquals("Query Processor", queryProcessor.getComponentName());
        System.out.println("Acc");
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
        ExecutionResult result = queryProcessor.executeQuery("SELECT * FROM users");

        assertNotNull(result);
        System.out.println(result);
        assertTrue(result.success());

        assertEquals("SELECT executed successfully", result.message());
        assertEquals("SELECT", result.operation());
        assertEquals(5, result.affectedRows()); // affectedRows harusnya = jumlah row
        
        // Cek datanya
        assertNotNull(result.rows());
        assertEquals(5, result.rows().size());
    }
}
