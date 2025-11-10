package com.apacy.concurrencycontrolmanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyControlManager extends DBMSComponent implements IConcurrencyControlManager {
    private final LockManager lockManager;
    private final TimestampManager timestampManager;
    
    private final Map<Integer, Transaction> transactionMap;
    private final AtomicInteger transactionCounter;

    public ConcurrencyControlManager() {
        super("Concurrency Control Manager");
        
        // Inisialisasi helper-nya
        // (Pilih salah satu sesuai implementasi wajib, misal LockManager)
        this.lockManager = new LockManager();
        this.timestampManager = new TimestampManager(); // Untuk bonus
        
        this.transactionMap = new ConcurrentHashMap<>();
        this.transactionCounter = new AtomicInteger(0);
    }

    public ConcurrencyControlManager(LockManager lockManager, TimestampManager timestampManager) {
        super("Concurrency Control Manager");
        this.lockManager = (lockManager != null) ? lockManager : new LockManager();
        this.timestampManager = timestampManager;
        this.transactionMap = new ConcurrentHashMap<>();
        this.transactionCounter = new AtomicInteger(0);
    }
    
    @Override
    public void initialize() throws Exception {
        // ... (Logika inisialisasi jika ada) ...
    }

    @Override
    public void shutdown() {
        // ... (Logika shutdown jika ada) ...
    }
    
    // 4. Implementasi method KONTRAK YANG BENAR
    
    @Override
    public int beginTransaction() {
        int txId = transactionCounter.incrementAndGet();
        Transaction tx = new Transaction(String.valueOf(txId));
        transactionMap.put(txId, tx);
        
        // TODO: Jika pakai timestamp, panggil tx.setTimestamp(timestampManager.generateTimestamp())
        
        return txId;
    }

    @Override
    public Response validateObject(String objectId, int transactionId, Action action) {
        // Ini adalah logika utamanya: mendelegasikan ke LockManager
        boolean allowed = false;
        
        if (action == Action.READ) {
            allowed = lockManager.acquireSharedLock(objectId, String.valueOf(transactionId));
        } else if (action == Action.WRITE) {
            allowed = lockManager.acquireExclusiveLock(objectId, String.valueOf(transactionId));
        }
        
        if (allowed) {
            return new Response(true, "Lock acquired");
        } else {
            // TODO: Implement deadlock detection logic here or in LockManager
            return new Response(false, "Resource locked by another transaction");
        }
    }
    
    @Override
    public void endTransaction(int transactionId, boolean commit) {
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

            // TODO: Panggil logic FRM.recover() untuk UNDO
        }
        
        // Selalu lepaskan lock, baik commit atau abort
        try {
            lockManager.releaseLocks(String.valueOf(transactionId));
        } catch (RuntimeException ignored) {}

        try {
            tx.terminate();
        } catch (RuntimeException ignored) {}

        transactionMap.remove(transactionId);
    }

    @Override
    public void logObject(Row object, int transactionId) {
        // Implementasi method ini sesuai spek
        // (Misal: mencatat 'before-image' dari 'object' ke 'transaction')
        Transaction tx = transactionMap.get(transactionId);
        if (tx != null) {
            try {
                tx.addLog(object);
            } catch (NoSuchMethodError | RuntimeException ignored) {
                // just ignore if dont exist
            }
        }
    }
}