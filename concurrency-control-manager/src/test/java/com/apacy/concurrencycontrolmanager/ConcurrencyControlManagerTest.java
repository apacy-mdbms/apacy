package com.apacy.concurrencycontrolmanager;

import com.apacy.common.dto.Response;
import com.apacy.common.enums.Action;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyControlManagerTest {

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

    /**
     * Helper method to access the private 'manager' field via reflection.
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
}