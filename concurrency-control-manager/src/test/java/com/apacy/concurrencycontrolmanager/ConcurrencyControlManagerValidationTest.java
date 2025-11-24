package com.apacy.concurrencycontrolmanager;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.*;
import com.apacy.common.enums.Action;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyControlManagerValidationTest {
    
    private ConcurrencyControlManagerValidation manager;
    
    @BeforeEach
    void setUp() {
        manager = new ConcurrencyControlManagerValidation();
    }
    
    @Test
    void testComponentName() {
        assertEquals("Concurrency Control Manager (Validation/OCC)", manager.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> manager.initialize());
    }
    
    @Test
    void testShutdown() {
        int t1 = manager.beginTransaction();

        // In OCC, validate always returns true (recording phase)
        Response r1 = manager.validateObjects(List.of("T_SH"), t1, Action.WRITE);
        assertNotNull(r1);
        assertTrue(tryIsSuccess(r1));
        
        assertDoesNotThrow(() -> manager.shutdown());

        // After shutdown/re-init, new transactions should work fine
        int t2 = manager.beginTransaction();
        Response r2 = manager.validateObjects(List.of("T_SH"), t2, Action.WRITE);
        assertNotNull(r2);
        assertTrue(tryIsSuccess(r2));
    }

    /**
     * Test: Simple successful commit scenario
     */
    @Test
    void testSimpleCommit() {
        int t1 = manager.beginTransaction();
        
        manager.validateObjects(List.of("A"), t1, Action.READ);
        manager.validateObjects(List.of("B"), t1, Action.WRITE);

        // Capture reference to check status later
        Transaction tx1 = getTransaction(t1);

        assertDoesNotThrow(() -> manager.endTransaction(t1, true));
        
        // Should be COMMITTED or PARTIALLY_COMMITTED
        assertNotEquals(Transaction.TransactionStatus.ABORTED, tx1.getStatus());
        assertNotEquals(Transaction.TransactionStatus.FAILED, tx1.getStatus());
    }

    /**
     * Test: Conflict - Read-Write Conflict (SILENT ABORT)
     * T1 reads X.
     * T2 writes X and Commits.
     * T1 tries to Commit -> Should FAIL validation.
     * Expected: No exception thrown, but T1 status set to ABORTED/FAILED.
     */
    @Test
    void testConflictReadWrite() {
        int t1 = manager.beginTransaction(); // T1 starts
        int t2 = manager.beginTransaction(); // T2 starts

        // Capture references early because endTransaction might remove them from the map
        Transaction tx1 = getTransaction(t1);
        Transaction tx2 = getTransaction(t2);

        // T1 reads X
        manager.validateObjects(List.of("X"), t1, Action.READ);

        // T2 writes X
        manager.validateObjects(List.of("X"), t2, Action.WRITE);

        // T2 Commits successfully (First one to commit usually wins)
        assertDoesNotThrow(() -> manager.endTransaction(t2, true));
        assertFalse(tx2.isAborted(), "T2 should commit successfully");

        // T1 tries to commit
        // Validation Logic: T2 wrote "X". T1 read "X". INTERSECTION -> Fail.
        // User Requirement: NO EXCEPTION THROWN.
        assertDoesNotThrow(() -> manager.endTransaction(t1, true));

        // ASSERTION: T1 must be marked as ABORTED or FAILED
        assertTrue(
            tx1.getStatus() == Transaction.TransactionStatus.ABORTED || 
            tx1.getStatus() == Transaction.TransactionStatus.FAILED, 
            "T1 should be silently aborted due to validation failure"
        );
    }

    /**
     * Test: No Conflict - Disjoint Sets
     * T1 reads A.
     * T2 writes B.
     * Both commit.
     */
    @Test
    void testNoConflictDisjoint() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Transaction tx1 = getTransaction(t1);
        Transaction tx2 = getTransaction(t2);

        manager.validateObjects(List.of("A"), t1, Action.READ);
        manager.validateObjects(List.of("B"), t2, Action.WRITE);

        assertDoesNotThrow(() -> manager.endTransaction(t1, true));
        assertDoesNotThrow(() -> manager.endTransaction(t2, true));

        assertFalse(tx1.isAborted());
        assertFalse(tx2.isAborted());
    }

    /**
     * Test: No Conflict - Write-Write (Blind Writes)
     * In standard OCC (and the provided ValidationManager logic), 
     * Write-Write usually doesn't conflict if they didn't read each other's data.
     */
    @Test
    void testWriteWriteNoConflict() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Transaction tx1 = getTransaction(t1);
        Transaction tx2 = getTransaction(t2);

        manager.validateObjects(List.of("X"), t1, Action.WRITE);
        manager.validateObjects(List.of("X"), t2, Action.WRITE);

        // T1 commits
        assertDoesNotThrow(() -> manager.endTransaction(t1, true));
        
        // T2 commits
        assertDoesNotThrow(() -> manager.endTransaction(t2, true));

        assertFalse(tx1.isAborted());
        assertFalse(tx2.isAborted());
    }

    /**
     * Test: Optimistic Nature
     * Unlike 2PL, requesting a resource held by another does NOT return false.
     */
    @Test
    void testOptimisticRead() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        // T1 "locks" (records write) X
        Response r1 = manager.validateObjects(List.of("X"), t1, Action.WRITE);
        assertTrue(tryIsSuccess(r1));

        // T2 tries to read X
        // In 2PL: This would fail/wait.
        // In OCC: This succeeds immediately.
        Response r2 = manager.validateObjects(List.of("X"), t2, Action.READ);
        assertNotNull(r2);
        assertTrue(tryIsSuccess(r2), "OCC should allow T2 to read even if T1 wrote");

        // Cleanup
        manager.endTransaction(t1, false);
        manager.endTransaction(t2, false);
    }

    @Test
    void testInitializeResetsTransactionCounter() throws Exception {
        int t1 = manager.beginTransaction();
        assertTrue(t1 > 0);
        manager.initialize();
        int t2 = manager.beginTransaction();
        // Counter reset check
        assertEquals(1, t2);
        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
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

    // --- Helper Methods (Reflection) ---

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

    /**
     * Retrieves the Transaction object via reflection.
     * We need this to check the status (ABORTED/COMMITTED) because endTransaction(true)
     * returns void and suppresses exceptions.
     */
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