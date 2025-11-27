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

class ConcurrencyControlManagerTimestampTest {

    private ConcurrencyControlManagerTimestamp manager;

    @BeforeEach
    void setUp() {
        manager = new ConcurrencyControlManagerTimestamp();
    }

    @Test
    void testComponentName() {
        assertEquals("Concurrency Control Manager Timestamp Ordering",
                     manager.getComponentName());
    }

    @Test
    void testInitialize() {
        assertDoesNotThrow(() -> manager.initialize());
    }

    @Test
    void testShutdown() {
        int t1 = manager.beginTransaction();

        Response r1 = manager.validateObjects(List.of("X"), t1, Action.READ);
        assertTrue(tryIsSuccess(r1));

        assertDoesNotThrow(() -> manager.shutdown());

        int t2 = manager.beginTransaction();
        Response r2 = manager.validateObjects(List.of("X"), t2, Action.WRITE);

        assertTrue(tryIsSuccess(r2));
    }

    @Test
    void testReadAllowed_TS_GE_WT() {
        int t1 = manager.beginTransaction();

        Response r = manager.validateObjects(List.of("X"), t1, Action.READ);
        assertTrue(tryIsSuccess(r));
    }

    @Test
    void testReadAbort_TS_LT_WT() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response w2 = manager.validateObjects(List.of("X"), t2, Action.WRITE);
        assertTrue(tryIsSuccess(w2));

        Response r1 = manager.validateObjects(List.of("X"), t1, Action.READ);
        assertFalse(tryIsSuccess(r1));
    }

    @Test
    void testWriteAllowed_TS_GE_RT_and_TS_GE_WT() {
        int t1 = manager.beginTransaction();

        Response r = manager.validateObjects(List.of("X"), t1, Action.READ);
        assertTrue(tryIsSuccess(r));

        Response w = manager.validateObjects(List.of("X"), t1, Action.WRITE);
        assertTrue(tryIsSuccess(w));
    }

    @Test
    void testWriteAbort_TS_LT_RT() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response r2 = manager.validateObjects(List.of("X"), t2, Action.READ);
        assertTrue(tryIsSuccess(r2));

        Response w1 = manager.validateObjects(List.of("X"), t1, Action.WRITE);
        assertFalse(tryIsSuccess(w1));
    }

    @Test
    void testWriteAbort_TS_LT_WT() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response w2 = manager.validateObjects(List.of("X"), t2, Action.WRITE);
        assertTrue(tryIsSuccess(w2));

        Response w1 = manager.validateObjects(List.of("X"), t1, Action.WRITE);
        assertFalse(tryIsSuccess(w1));
    }

    @Test
    void testAbortBlocksFurtherOperations() {
        int t1 = manager.beginTransaction();
        int t2 = manager.beginTransaction();

        Response w2 = manager.validateObjects(List.of("X"), t2, Action.WRITE);
        assertTrue(tryIsSuccess(w2));

        Response w1 = manager.validateObjects(List.of("X"), t1, Action.WRITE);
        assertFalse(tryIsSuccess(w1));

        Response again = manager.validateObjects(List.of("X"), t1, Action.READ);
        assertFalse(tryIsSuccess(again));
    }

    @Test
    void testMultipleObjectValidation() {
        int t1 = manager.beginTransaction();

        Response r = manager.validateObjects(List.of("A", "B", "C"), t1, Action.READ);
        assertTrue(tryIsSuccess(r));

        Response w = manager.validateObjects(List.of("A", "B"), t1, Action.WRITE);
        assertTrue(tryIsSuccess(w));
    }

    @Test
    void testSimpleCommit() {
        int t1 = manager.beginTransaction();
        Transaction tx = getTransaction(t1);

        manager.validateObjects(List.of("A"), t1, Action.READ);
        manager.validateObjects(List.of("B"), t1, Action.WRITE);

        assertDoesNotThrow(() -> manager.endTransaction(t1, true));

        assertFalse(tx.isAborted());
        assertNotEquals(Transaction.TransactionStatus.FAILED, tx.getStatus());
    }

    @Test
    void testInitializeResetsTransactionCounter() {
        int t1 = manager.beginTransaction();
        assertTrue(t1 > 0);

        manager.initialize();

        int t2 = manager.beginTransaction();
        assertEquals(1, t2);

        assertDoesNotThrow(() -> manager.endTransaction(t2, false));
    }

    @Test
    void testLogObject() {
        int t1 = manager.beginTransaction();

        Map<String, Object> m = new HashMap<>();
        m.put("key", "value");

        Row row = new Row(m);

        assertDoesNotThrow(() -> manager.logObject(row, t1));

        Transaction tx = getTransaction(t1);
        assertNotNull(tx);

        List<Row> logs = tx.getLoggedObjects();
        assertEquals(1, logs.size());
        assertEquals(row, logs.get(0));

        assertDoesNotThrow(() -> manager.endTransaction(t1, true));
    }

    private static boolean tryIsSuccess(Response r) {
        if (r == null) return false;
        try {
            Field field = r.getClass().getDeclaredField("isAllowed");
            field.setAccessible(true);
            return (boolean) field.get(r);
        } catch (Exception ignored) {
            return false;
        }
    }

    private Transaction getTransaction(int id) {
        if (manager == null) return null;
        try {
            Field mapField = manager.getClass().getDeclaredField("transactionMap");
            mapField.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<Integer, Transaction> map = (Map<Integer, Transaction>) mapField.get(manager);

            return map.get(id);
        } catch (Exception ignored) {
            return null;
        }
    }
}
