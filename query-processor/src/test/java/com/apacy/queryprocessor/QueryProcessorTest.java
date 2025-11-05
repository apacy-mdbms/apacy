package com.apacy.queryprocessor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class QueryProcessorTest {
    
    private QueryProcessor queryProcessor;
    
    @BeforeEach
    void setUp() {
        queryProcessor = new QueryProcessor();
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
}
