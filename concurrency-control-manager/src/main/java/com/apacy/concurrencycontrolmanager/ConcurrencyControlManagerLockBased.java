package com.apacy.concurrencycontrolmanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.enums.Action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConcurrencyControlManagerLockBased extends DBMSComponent implements IConcurrencyControlManagerAlgorithm {
    private final LockManager lockManager;
    private final TimestampManager timestampManager;
    private final Map<Integer, Transaction> transactionMap;
    
    private int transactionCounter;

    public ConcurrencyControlManagerLockBased() {
        super("Concurrency Control Manager");

        this.lockManager = new LockManager();
        this.timestampManager = new TimestampManager();

        this.transactionMap = new HashMap<>();
        this.transactionCounter = 0;
    }

    public ConcurrencyControlManagerLockBased(LockManager lockManager, TimestampManager timestampManager) {
        super("Concurrency Control Manager");
        this.lockManager = (lockManager != null) ? lockManager : new LockManager();
        this.timestampManager = timestampManager;
        this.transactionMap = new HashMap<>();
        this.transactionCounter = 0;
    }

    @Override
    public synchronized void initialize() {
        transactionMap.clear();
        transactionCounter = 0;
    }

    @Override
    public synchronized void shutdown() {
        // PENTING: copy keys ke List dulu agar tidak terjadi ConcurrentModificationException 
        // saat remove item dalam loop
        List<Integer> keys = new ArrayList<>(transactionMap.keySet());

        for (Integer txId : keys) {
            Transaction tx = transactionMap.get(txId);
            if (tx == null) continue;

            try {
                if (!tx.isFailed() && !tx.isAborted() && !tx.isCommitted()) {
                    tx.setFailed();
                    tx.abort();
                }
            } catch (RuntimeException ignored) {}

            try {
                lockManager.releaseLocks(tx);
            } catch (RuntimeException ignored) {}

            try {
                tx.terminate();
            } catch (RuntimeException ignored) {}

            transactionMap.remove(txId);
        }
    }

    @Override
    public synchronized int beginTransaction() {
        transactionCounter++; 
        int txId = transactionCounter;
        
        Transaction tx = new Transaction(String.valueOf(txId));
        transactionMap.put(txId, tx);

        try {
            if (timestampManager != null) {
                long ts = timestampManager.generateTimestamp();
                tx.setTimestamp(ts);
            }
        } catch (RuntimeException ignored) {}

        return txId;
    }

    @Override
    public synchronized Response validateObject(String objectId, int transactionId, Action action) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx == null) {
            return new Response(false, "Transaction not found: " + transactionId);
        }

        if (tx.isAborted()) {
            return new Response(false, "Transaction " + transactionId + " was aborted (Wounded).");
        }

        boolean allowed = false;

        if (action == Action.READ) {
            allowed = lockManager.acquireSharedLock(objectId, tx);
        } else if (action == Action.WRITE) {
            allowed = lockManager.acquireExclusiveLock(objectId, tx);
        }

        if (allowed) {
            return new Response(true, "Lock acquired");
        } else {
            if (tx.isAborted()) {
                return new Response(false, "Transaction " + transactionId + " was aborted (Wounded).");
            }
            return new Response(false, "Resource locked by another transaction (Wait)");
        }
    }

    @Override
    public synchronized Response validateObjects(List<String> objectIds, int transactionId, Action action) {
        for (String objectId : objectIds) {
            Response response = validateObject(objectId, transactionId, action);

            if (!response.isAllowed()) {
                return new Response(false,
                        "Failed on object '" + objectId + "': " + response.reason());
            }
        }

        return new Response(true, "All locks acquired for transaction " + transactionId);
    }


    @Override
    public synchronized void endTransaction(int transactionId, boolean commit) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx == null) {
            throw new IllegalArgumentException("Transaction not found: " + transactionId);
        }

        if (commit) {
            try {
                tx.setPartiallyCommitted();
                tx.commit();
            } catch (RuntimeException e) {
                try {
                    tx.setFailed();
                    tx.abort();
                } catch (RuntimeException ignored) {}
            }
        } else {
            try {
                tx.setFailed();
                tx.abort();
            } catch (RuntimeException ignored) {}
        }

        try {
            lockManager.releaseLocks(tx);
        } catch (RuntimeException ignored) {}

        try {
            tx.terminate();
        } catch (RuntimeException ignored) {}

        transactionMap.remove(transactionId);
    }

    @Override
    public synchronized void logObject(Row object, int transactionId) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx != null) {
            try {
                tx.addLog(object);
            } catch (NoSuchMethodError | RuntimeException ignored) {
                // just ignore if don't exist
            }
        }
    }
}