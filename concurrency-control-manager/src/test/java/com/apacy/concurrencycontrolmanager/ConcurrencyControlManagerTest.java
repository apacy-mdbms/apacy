package com.apacy.concurrencycontrolmanager;

import com.apacy.common.dto.Response;
import com.apacy.common.enums.Action;
import com.apacy.concurrencycontrolmanager.mocks.MockFailureRecoveryManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyControlManagerTest {

    private MockFailureRecoveryManager frm;

    @BeforeEach
    void setup() {
        frm = new MockFailureRecoveryManager();
    }

    /**
     * Test that passing "lock" initializes the LockBased strategy.
     */
    @Test
    void testStrategySelectionLock() {
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("lock");
        Object internalManager = getInternalManager(ccm);

        assertNotNull(internalManager);
        assertInstanceOf(ConcurrencyControlManagerLockBased.class, internalManager, 
            "Strategy 'lock' should instantiate ConcurrencyControlManagerLockBased");
    }

    /**
     * Test that passing "timestamp" initializes the Timestamp strategy.
     */
    @Test
    void testStrategySelectionTimestamp() {
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("timestamp");
        Object internalManager = getInternalManager(ccm);

        assertNotNull(internalManager);
        assertInstanceOf(ConcurrencyControlManagerTimestamp.class, internalManager, 
            "Strategy 'timestamp' should instantiate ConcurrencyControlManagerTimestamp");
    }

    /**
     * Test that passing "validation" initializes the Validation strategy.
     */
    @Test
    void testStrategySelectionValidation() {
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("validation");
        Object internalManager = getInternalManager(ccm);

        assertNotNull(internalManager);
        assertInstanceOf(ConcurrencyControlManagerValidation.class, internalManager, 
            "Strategy 'validation' should instantiate ConcurrencyControlManagerValidation");
    }

    /**
     * Test that passing an unknown string throws an exception.
     */
    @Test
    void testInvalidStrategy() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new ConcurrencyControlManager("invalid_algo");
        });

        assertTrue(exception.getMessage().contains("Invalid Algorithm"), 
            "Exception message should mention invalid algorithm");
    }

    /**
     * Test that the wrapper correctly delegates methods to the underlying strategy.
     * We use a simple scenario (beginTransaction) to prove the wire-up works.
     */
    @Test
    void testDelegationWorks() {
        // Use lock based for this smoke test
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("lock");

        // 1. Test beginTransaction delegation
        int txId = ccm.beginTransaction();
        assertTrue(txId > 0, "Wrapper should return valid transaction ID from delegate");

        // 2. Test validateObject delegation
        Response response = ccm.validateObject("TEST_OBJ", txId, Action.WRITE);
        assertNotNull(response, "Wrapper should return response from delegate");
        
        // 3. Test endTransaction delegation
        assertDoesNotThrow(() -> ccm.endTransaction(txId, false));
    }

    @Test
    void testFRM_beginTransaction_logsStart() throws Exception {
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("lock", frm);

        int tx = ccm.beginTransaction();

        assertEquals(1, frm.getTransactionLogs().size());
        assertEquals("START", frm.getTransactionLogs().get(0).getLifecycleEvent());
    }

    @Test
    void testFRM_validateObject_logsValidate() throws Exception {
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("timestamp");
        injectFRM(ccm, frm);

        int tx = ccm.beginTransaction();
        ccm.validateObject("TABLE::users", tx, Action.READ);

        boolean hasValidate = frm.getTransactionLogs()
            .stream()
            .anyMatch(l -> l.getLifecycleEvent().contains("VALIDATE"));

        assertTrue(hasValidate);
    }

    @Test
    void testFRM_endTransaction_commit_logsCommit() throws Exception {
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("validation");
        injectFRM(ccm, frm);

        int tx = ccm.beginTransaction();
        ccm.endTransaction(tx, true);

        boolean hasCommit = frm.getTransactionLogs()
            .stream()
            .anyMatch(l -> l.getLifecycleEvent().equals("COMMIT"));

        assertTrue(hasCommit);
    }

    @Test
    void testFRM_endTransaction_abort_logsAbort() throws Exception {
        ConcurrencyControlManager ccm = new ConcurrencyControlManager("lock");
        injectFRM(ccm, frm);

        int tx = ccm.beginTransaction();
        ccm.endTransaction(tx, false);

        boolean hasAbort = frm.getTransactionLogs()
            .stream()
            .anyMatch(l -> l.getLifecycleEvent().equals("ABORT"));

        assertTrue(hasAbort);
    }

    /**
     * Helper: Access internal manager strategy for existing tests.
     */
    private Object getInternalManager(ConcurrencyControlManager ccm) {
        try {
            Field field = ConcurrencyControlManager.class.getDeclaredField("manager");
            field.setAccessible(true);
            return field.get(ccm);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Failed to access private 'manager' field via reflection: " + e.getMessage());
            return null;
        }
    }

    private void injectFRM(ConcurrencyControlManager ccm, MockFailureRecoveryManager frm) {
        try {
            Field field = ConcurrencyControlManager.class.getDeclaredField("failureRecoveryManager");
            field.setAccessible(true);
            field.set(ccm, frm);
        } catch (Exception e) {
            fail("Failed to inject failureRecoveryManager: " + e.getMessage());
        }
    }
}
