package com.apacy.concurrencycontrolmanager;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.*;
import com.apacy.common.enums.Action;
import com.apacy.concurrencycontrolmanager.mocks.MockTimestampManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyControlManagerTest {
    
    private ConcurrencyControlManager manager;
    
    @BeforeEach
    void setUp() {
        manager = new ConcurrencyControlManager(new LockManager(), new MockTimestampManager());
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
        int t1 = manager.beginTransaction();

        Response r1 = manager.validateObject("T_SH", t1, Action.WRITE);
        assertNotNull(r1);
        assertTrue(tryIsSuccess(r1), "Should acquire Exclusive lock");
        assertDoesNotThrow(() -> manager.shutdown());

        int t2 = manager.beginTransaction();
        
        Response r2 = manager.validateObject("T_SH", t2, Action.WRITE);
        assertNotNull(r2);
        assertTrue(tryIsSuccess(r2), "Should acquire Exclusive lock");
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

        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
        assertDoesNotThrow(() -> manager.endTransaction(t3, false));
    }

    @Test
    void testSharedReadsAllowMultipleTransactions() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response r1 = manager.validateObject("S_RES", t1, Action.READ);
        Response r2 = manager.validateObject("S_RES", t2, Action.READ);

        assertNotNull(r1);
        assertNotNull(r2);
        assertTrue(tryIsSuccess(r1));
        assertTrue(tryIsSuccess(r2));

        assertDoesNotThrow(() -> manager.endTransaction(t1, false)); 
        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
    }

    @Test
    void testAbortReleasesLocks() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response t1w = manager.validateObject("Z_RES", t1, Action.WRITE);
        assertNotNull(t1w);
        assertTrue(tryIsSuccess(t1w));

        Response t2r = manager.validateObject("Z_RES", t2, Action.READ);
        assertNotNull(t2r);
        assertFalse(tryIsSuccess(t2r));

        assertDoesNotThrow(() -> manager.endTransaction(t1, false));

        Response t2rAfter = manager.validateObject("Z_RES", t2, Action.READ);
        assertNotNull(t2rAfter);
        assertTrue(tryIsSuccess(t2rAfter));

        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
    }

    @Test
    void testInitializeResetsTransactionCounter() throws Exception {
        int t1 = manager.beginTransaction();
        assertTrue(t1 > 0);
        manager.initialize();
        int t2 = manager.beginTransaction();
        assertEquals(1, t2);

        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
    }

    @Test
    void testWoundScenario() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response r2 = manager.validateObject("WOUND_RES", t2, Action.WRITE);
        assertNotNull(r2);
        assertTrue(tryIsSuccess(r2), "Younger transaction should acquire the lock");

        Response r1 = manager.validateObject("WOUND_RES", t1, Action.READ);
        assertNotNull(r1);

        assertFalse(tryIsSuccess(r1), "Older T's request should fail (WAIT)");
        assertEquals(Transaction.TransactionStatus.ABORTED, getTransactionStatus(t2), "Younger transaction should've been 'Wounded' (ABORTED)");

        assertDoesNotThrow(() -> manager.endTransaction(t1, false)); 
        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
    }

    @Test
    void testLockUpgradeScenario() {
        int t1 = manager.beginTransaction();
        Response read = manager.validateObject("UP_RES", t1, Action.READ);
        assertNotNull(read);
        assertTrue(tryIsSuccess(read), "Should acquire shared lock");

        Response write = manager.validateObject("UP_RES", t1, Action.WRITE);
        assertNotNull(write);
        assertTrue(tryIsSuccess(write), "Should successfully upgrade to exclusive lock");

        assertDoesNotThrow(() -> manager.endTransaction(t1, true));
    }

    @Test
    void testLockConflictScenario() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response r1 = manager.validateObject("RES_SX", t1, Action.READ);
        assertNotNull(r1);
        assertTrue(tryIsSuccess(r1), "Older T's should acquire the shared lock");

        Response r2 = manager.validateObject("RES_SX", t2, Action.WRITE);
        assertNotNull(r2);
        assertFalse(tryIsSuccess(r2), "Younger transaction should fail to acquire the exclusive lock (WAIT)");

        assertDoesNotThrow(() -> manager.endTransaction(t1, false)); 
        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
    }

    @Test
    void testEdgeCases() {
        int t1 = manager.beginTransaction();
        
        assertDoesNotThrow(() -> manager.endTransaction(t1, false));
        assertThrows(IllegalArgumentException.class, () -> manager.endTransaction(t1, true));
        assertThrows(IllegalArgumentException.class, () -> manager.endTransaction(99999, true));
    }

    @Test
    void testLogObject() {
        int t1 = manager.beginTransaction();

        Map<String, Object> map = new HashMap<>();
        map.put("name", "Alice");
        map.put("age", 30);

        Row myRow = new Row(map);

        assertDoesNotThrow(() -> manager.logObject(myRow, t1));

        Transaction tx = getTransaction(t1);
        assertNotNull(tx, "Transaction should exist");

        List<Row> loggedRows = tx.getLoggedObjects();
        assertNotNull(loggedRows, "Logged objects list shouldn't be null");
        assertEquals(1, loggedRows.size(), "Should have one logged object");
        assertEquals(myRow, loggedRows.get(0), "The logged object should be the one we passed");

        assertDoesNotThrow(() -> manager.endTransaction(t1, true));
    }

    @Test
    void testLogObjectInvalidTransaction() {
        Row myRow = null;
        assertDoesNotThrow(() -> manager.logObject(myRow, 99999));
    }

    private static boolean tryIsSuccess(Response r ) {
        if (r == null) return false;
        try {
            Field field = r.getClass().getDeclaredField("isAllowed");
            field.setAccessible(true);
            return (boolean) field.get(r);
        } catch (ReflectiveOperationException | ClassCastException ignored) {
            return false;
        }
    }

    private Transaction.TransactionStatus getTransactionStatus(int transactionId) {
        if (manager == null) return null;
        try {
            Field mapField = manager.getClass().getDeclaredField("transactionMap");
            mapField.setAccessible(true);
 
            @SuppressWarnings("unchecked")
            java.util.Map<Integer, Transaction> transactionMap = (java.util.Map<Integer, Transaction>) mapField.get(manager);
            
            Transaction tx = transactionMap.get(transactionId);
            if (tx != null) {
                return tx.getStatus();
            }
        } catch (ReflectiveOperationException | ClassCastException ignored) {
        }
        return null;
    }

    private Transaction getTransaction(int transactionId) {
        if (manager == null) return null;
        try {
            Field mapField = manager.getClass().getDeclaredField("transactionMap");
            mapField.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<Integer, Transaction> transactionMap = (Map<Integer, Transaction>) mapField.get(manager);

            return transactionMap.get(transactionId);
        } catch (ReflectiveOperationException | ClassCastException ignored) {
            return null;
        }
    }
}
