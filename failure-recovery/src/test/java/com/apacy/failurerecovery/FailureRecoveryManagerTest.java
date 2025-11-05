package com.apacy.failurerecovery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class FailureRecoveryManagerTest {
    
    private FailureRecoveryManager failureRecoveryManager;
    
    @BeforeEach
    void setUp() {
        failureRecoveryManager = new FailureRecoveryManager();
    }
    
    @Test
    void testComponentName() {
        assertEquals("Failure Recovery Manager", failureRecoveryManager.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> failureRecoveryManager.initialize());
    }
    
    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> failureRecoveryManager.shutdown());
    }
}
