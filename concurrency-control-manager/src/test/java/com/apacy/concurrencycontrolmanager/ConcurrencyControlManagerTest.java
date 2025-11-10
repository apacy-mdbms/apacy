package com.apacy.concurrencycontrolmanager;

import java.lang.reflect.Field;
import com.apacy.common.dto.Response;
import com.apacy.common.enums.Action;
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

    @Test 
    void testTransactionScenarios() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();
        assertEquals(t1 + 1, t2);

        Response r1 = manager.validateObject("R1", t1, Action.READ);
        assertNotNull(r1);

        assertTrue(tryIsSuccess(r1));

        Response r2 = manager.validateObject("W1", t1, Action.WRITE);
        assertNotNull(r2);

        int t3 = manager.beginTransaction();
        Response t1w = manager.validateObject("X", t1, Action.WRITE);
        assertNotNull(t1w);
        Response t3r = manager.validateObject("X", t3, Action.READ);
        assertNotNull(t3r);

        boolean t1wSuccess = tryIsSuccess(t1w);
        boolean t3rSuccess = tryIsSuccess(t3r);
        assertTrue(t1wSuccess, "t1 should acquire exclusive lock");
        assertFalse(t3rSuccess, "t3 should not acquire shared lock while exclusive held");

        assertDoesNotThrow(() -> manager.endTransaction(t1, true));

        Response t3readAfter = manager.validateObject("X", t3, Action.READ);
        assertNotNull(t3readAfter);
        assertTrue(tryIsSuccess(t3readAfter));

        assertThrows(IllegalArgumentException.class, () -> manager.endTransaction(99999, true));
    }

    private static boolean tryIsSuccess(Response r ) {
        if (r == null) return false;
        try {
            Field field = r.getClass().getDeclaredField("isAllowed");
            field.setAccessible(true);
            return (boolean) field.get(r);
        } catch (ReflectiveOperationException | ClassCastException ignored) {
            return true;
        }
    }
}
