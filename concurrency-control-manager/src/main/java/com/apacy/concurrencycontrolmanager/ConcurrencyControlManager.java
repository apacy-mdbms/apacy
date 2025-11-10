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
            // Sebaiknya throw exception
            return; 
        }
        
        if (commit) {
            tx.setStatus(Transaction.TransactionStatus.COMMITTED);
        } else {
            tx.setStatus(Transaction.TransactionStatus.ABORTED);
            // TODO: Panggil logic FRM.recover() untuk UNDO
        }
        
        // Selalu lepaskan lock, baik commit atau abort
        lockManager.releaseLocks(tx); // Panggil dengan objek Transaction
        transactionMap.remove(transactionId);
    }

    @Override
    public void logObject(Row object, int transactionId) {
        // Implementasi method ini sesuai spek
        // (Misal: mencatat 'before-image' dari 'object' ke 'transaction')
        Transaction tx = transactionMap.get(transactionId);
        if (tx != null) {
            // tx.addLog(object); // Perlu menambah method ini di Transaction.java
        }
    }
}