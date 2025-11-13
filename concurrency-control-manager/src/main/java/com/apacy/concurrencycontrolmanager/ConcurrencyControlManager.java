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
        // Reset internal state biar CCMnya clean
        transactionMap.clear();
        transactionCounter.set(0);
    }

    @Override
    public void shutdown() {
        for (Integer txId : transactionMap.keySet()) {
            Transaction tx = transactionMap.get(txId);
            if (tx == null) continue;

            // kalo belum final (aborted, failed, commited) paksa fail terus abort
            try {
                if (!tx.isFailed() && !tx.isAborted() && !tx.isCommitted()) {
                    tx.setFailed();
                    tx.abort();
                }
            } catch (RuntimeException ignored) {}

            // release locknya
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
    public int beginTransaction() {
        int txId = transactionCounter.incrementAndGet();
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
    public Response validateObject(String objectId, int transactionId, Action action) {
        // Ambil objek Transaction yang sesuai
        Transaction tx = transactionMap.get(transactionId);
        if (tx == null) {
            return new Response(false, "Transaction not found: " + transactionId);
        }

        // Cek apakah transaksi ini sudah di-abort (berarti kasus WOUND)
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
            // Cek lagi jika transaksi di-abort saat proses acquire (kasus WOUND)
            if (tx.isAborted()) {
                return new Response(false, "Transaction " + transactionId + " was aborted (Wounded).");
            }
            // Jika tidak di-abort, berarti ini kasus WAIT
            return new Response(false, "Resource locked by another transaction (Wait)");
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
            lockManager.releaseLocks(tx);
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