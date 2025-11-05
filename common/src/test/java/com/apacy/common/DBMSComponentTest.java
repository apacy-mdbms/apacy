package com.apacy.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DBMSComponentTest {
    
    private static class TestComponent extends DBMSComponent {
        private boolean initialized = false;
        private boolean shutdown = false;
        
        public TestComponent() {
            super("Test Component");
        }
        
        @Override
        public void initialize() {
            initialized = true;
        }
        
        @Override
        public void shutdown() {
            shutdown = true;
        }
        
        public boolean isInitialized() {
            return initialized;
        }
        
        public boolean isShutdown() {
            return shutdown;
        }
    }
    
    @Test
    void testComponentName() {
        TestComponent component = new TestComponent();
        assertEquals("Test Component", component.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        TestComponent component = new TestComponent();
        assertFalse(component.isInitialized());
        component.initialize();
        assertTrue(component.isInitialized());
    }
    
    @Test
    void testShutdown() {
        TestComponent component = new TestComponent();
        assertFalse(component.isShutdown());
        component.shutdown();
        assertTrue(component.isShutdown());
    }
}
