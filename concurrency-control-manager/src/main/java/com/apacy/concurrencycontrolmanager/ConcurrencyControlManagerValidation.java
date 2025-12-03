package com.apacy.concurrencycontrolmanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.Response;
import com.apacy.common.dto.Row;
import com.apacy.common.enums.Action;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Concurrency Control Manager implementing Validation Based Protocol (Optimistic).
 * 1. validateObject() selalu return ALLOWED (true)
 * 2. conflict check hanya pada endTransaction().
 */
public class ConcurrencyControlManagerValidation extends DBMSComponent implements IConcurrencyControlManagerAlgorithm {

    private final ValidationManager validationManager;
    private final Map<Integer, Transaction> transactionMap;
    private int transactionCounter;

    public ConcurrencyControlManagerValidation() {
        super("Concurrency Control Manager (Validation/OCC)");
        this.validationManager = new ValidationManager();
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
        transactionMap.clear();
    }

    @Override
    public synchronized int beginTransaction() {
        transactionCounter++;
        int txId = transactionCounter;

        // Create transaction object
        Transaction tx = new Transaction(String.valueOf(txId));
        transactionMap.put(txId, tx);

        // Notify Validation Manager to start tracking timestamp
        validationManager.onTransactionStart(txId);

        return txId;
    }

    /**
     * Validation Based berasumsi read selalu ALLOWED
     * cukup RECORD read/written untuk validation nanti
     */
    @Override
    public synchronized Response validateObject(String objectId, int transactionId, Action action) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx == null) {
            return new Response(false, "Transaction not found: " + transactionId);
        }

        // Record the access in the Validation Manager
        if (action == Action.READ) {
            validationManager.recordRead(transactionId, objectId);
        } else if (action == Action.WRITE) {
            validationManager.recordWrite(transactionId, objectId);
        }

        // Optimistic: Always allow execution during Read Phase
        return new Response(true, "Recorded for validation (Optimistic)");
    }

    @Override
    public synchronized Response validateObjects(List<String> objectIds, int transactionId, Action action) {
        // Bulk record
        for (String objectId : objectIds) {
            validateObject(objectId, transactionId, action);
        }
        return new Response(true, "All objects recorded (Optimistic)");
    }

    /**
     * CRITICAL point for Validation Based
     * The Validation Phase happens here.
     */
    @Override
    public synchronized void endTransaction(int transactionId, boolean commit) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx == null) {
            throw new IllegalArgumentException("Transaction not found: " + transactionId);
        }

        if (commit) {
            // --- VALIDATION PHASE ---
            boolean isValid = validationManager.validate(transactionId);

            if (isValid) {
                // --- WRITE PHASE (Commit) ---
                try {
                    tx.setPartiallyCommitted();
                    tx.commit();
                } catch (Exception e) {
                    try {
                        validationManager.onAbort(transactionId); // Cleanup
                        tx.setFailed();
                        tx.abort();
                    } catch (RuntimeException ignored) {}
                }
            } else {
                // --- VALIDATION FAILED ---
                try {
                    validationManager.onAbort(transactionId); // Cleanup aborted state
                    tx.setFailed();
                    tx.abort();
                } catch (RuntimeException ignored) {}
            }
        } else {
            try {
                // Explicit Rollback requested
                validationManager.onAbort(transactionId);
                tx.setFailed();
                tx.abort();
            } catch (RuntimeException ignored) {}
        }

        // Remove from active map
        transactionMap.remove(transactionId);
    }
    
    @Override
    public synchronized void logObject(Row object, int transactionId) {
        Transaction tx = transactionMap.get(transactionId);
        if (tx != null) {
            tx.addLog(object);
        }
    }
}