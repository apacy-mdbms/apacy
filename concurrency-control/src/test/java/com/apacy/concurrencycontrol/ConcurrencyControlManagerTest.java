package com.apacy.concurrencycontrol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyControlManagerTest {
    
    private ConcurrencyControlManager concurrencyControlManager;
    
    @BeforeEach
    void setUp() {
        concurrencyControlManager = new ConcurrencyControlManager();
    }
    
    @Test
    void testComponentName() {
        assertEquals("Concurrency Control Manager", concurrencyControlManager.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> concurrencyControlManager.initialize());
    }
    
    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> concurrencyControlManager.shutdown());
    }
}
