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
}
