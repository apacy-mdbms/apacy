package com.apacy.concurrencycontrolmanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.Response;
import com.apacy.common.dto.Row;
import com.apacy.common.enums.Action;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConcurrencyControlManagerTimestamp extends DBMSComponent implements IConcurrencyControlManagerAlgorithm {

    private final TimestampManager timestampManager;
    private final Map<Integer, Transaction> transactionMap;
    private long globalTS;

    public ConcurrencyControlManagerTimestamp() {
        super("Concurrency Control Manager Timestamp Ordering");
        this.timestampManager = new TimestampManager();
        this.transactionMap = new HashMap<>();
        this.globalTS = 0;
    }

    @Override
    public synchronized void initialize() {
        transactionMap.clear();
        globalTS = 0;
    }

    @Override
    public synchronized void shutdown() {
        transactionMap.clear();
    }

    @Override
    public synchronized int beginTransaction() {
        globalTS++;
        int id = (int) globalTS;

        Transaction tx = new Transaction(String.valueOf(id));

        tx.setTimestamp(id);

        transactionMap.put(id, tx);
        return id;
    }

    // Timestamp Ordering
    @Override
    public synchronized Response validateObject(String objectId, int transactionId, Action action) {
        Transaction tx = transactionMap.get(transactionId);

        if (tx == null) {
            return new Response(false, "Transaction not found");
        }

        if (tx.isAborted()) {
            return new Response(false, "Transaction already aborted");
        }

        long ts = tx.getTimestamp();
        long RT = timestampManager.getReadTimestamp(objectId);
        long WT = timestampManager.getWriteTimestamp(objectId);

        if (action == Action.READ) {
            if (ts < WT) {
                tx.setFailed();
                tx.abort();
                return new Response(false, "ABORT: TS(T)=" + ts + " < WT(" + objectId + ")=" + WT);
            }

            timestampManager.updateReadTimestamp(objectId, ts);
            return new Response(true, "READ allowed");
        }

        if (action == Action.WRITE) {
            if (ts < RT || ts < WT) {
                tx.setFailed();
                tx.abort();
                return new Response(false, "ABORT: TS(T)=" + ts + " < max(RT,WT) of " + objectId + " (RT=" + RT + ", WT=" + WT + ")");
            }

            timestampManager.updateWriteTimestamp(objectId, ts);
            return new Response(true, "WRITE allowed");
        }

        return new Response(false, "Unknown action");
    }

    @Override
    public synchronized Response validateObjects(List<String> objectIds, int transactionId, Action action) {
        for (String objectId : objectIds) {
            Response res = validateObject(objectId, transactionId, action);
            if (!res.isAllowed()) return res;
        }
        return new Response(true, "All operations allowed");
    }

    @Override
    public synchronized void endTransaction(int transactionId, boolean commit) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx == null) return;

        if (tx.isAborted()) {
            tx.terminate();
            transactionMap.remove(transactionId);
            return;
        }

        if (commit) {
            tx.setPartiallyCommitted();
            tx.commit();
        } else {
            tx.setFailed();
            tx.abort();
        }

        tx.terminate();
        transactionMap.remove(transactionId);
    }

    @Override
    public synchronized void logObject(Row row, int transactionId) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx != null) {
            try { tx.addLog(row);
            } catch (RuntimeException ignored) {}
        }
    }
}