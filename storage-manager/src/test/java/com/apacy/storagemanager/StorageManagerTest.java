package com.apacy.storagemanager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class StorageManagerTest {
    
    private StorageManager storageManager;
    
    @BeforeEach
    void setUp() {
        storageManager = new StorageManager("Bruh");
    }
    
    @Test
    void testComponentName() {
        assertEquals("Storage Manager", storageManager.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> storageManager.initialize());
    }
    
    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> storageManager.shutdown());
    }
}
