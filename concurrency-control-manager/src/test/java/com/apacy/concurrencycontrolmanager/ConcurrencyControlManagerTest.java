package com.apacy.concurrencycontrolmanager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyControlManagerTest {
    
    private ConcurrencyControlManager manager;
    
    @BeforeEach
    void setUp() {
        manager = new ConcurrencyControlManager();
    }
    
    @Test
    void testComponentName() {
        assertEquals("Concurrency Control Manager", manager.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> manager.initialize());
    }
    
    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> manager.shutdown());
    }
}
