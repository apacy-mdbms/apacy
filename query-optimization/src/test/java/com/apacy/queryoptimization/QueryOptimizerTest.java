package com.apacy.queryoptimization;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class QueryOptimizerTest {
    
    private QueryOptimizer queryOptimizer;
    
    @BeforeEach
    void setUp() {
        queryOptimizer = new QueryOptimizer();
    }
    
    @Test
    void testComponentName() {
        assertEquals("Query Optimizer", queryOptimizer.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> queryOptimizer.initialize());
    }
    
    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> queryOptimizer.shutdown());
    }
}
