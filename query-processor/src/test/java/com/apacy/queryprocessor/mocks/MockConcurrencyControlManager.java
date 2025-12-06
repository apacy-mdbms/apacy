package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.Response;
import com.apacy.common.dto.Row;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;

import java.util.List;

/**
 * Mock implementasi dari IConcurrencyControlManager untuk keperluan testing Query Processor.
 * * Fitur:
 * 1. Stubbing: Mengatur hasil validasi (Allow/Deny) untuk simulasi locking.
 * 2. Spying: Melacak transaction lifecycle (begin/end) dan objek yang divalidasi.
 */
public class MockConcurrencyControlManager implements IConcurrencyControlManager {

    // --- Configuration (Stubbing) ---
    private boolean shouldAllowValidation = true;
    private String validationMessage = "Mock: Access Allowed";
    private int nextTransactionId = 100;

    // --- State (Spying) ---
    private int beginTransactionCallCount = 0;
    
    private int endTransactionCallCount = 0;
    private Integer lastEndedTransactionId;
    private Boolean lastEndCommitStatus;

    private int validateObjectCallCount = 0;
    private String lastValidatedObjectId;
    private Action lastValidatedAction;
    private int lastValidatedTransactionId;

    private int logObjectCallCount = 0;
    private Row lastLoggedObject;

    // --- Configuration Methods ---

    /**
     * Mengatur apakah validateObject() akan mengembalikan true (allowed) atau false.
     */
    public void setShouldAllowValidation(boolean allow) {
        this.shouldAllowValidation = allow;
        this.validationMessage = allow ? "Mock: Access Allowed" : "Mock: Access Denied";
    }

    /**
     * Mengatur ID transaksi berikutnya yang akan dikembalikan oleh beginTransaction().
     */
    public void setNextTransactionId(int nextId) {
        this.nextTransactionId = nextId;
    }

    // --- Interface Implementation ---

    @Override
    public int beginTransaction() {
        this.beginTransactionCallCount++;
        return this.nextTransactionId++;
    }

    @Override
    public void logObject(Row object, int transactionId) {
        this.logObjectCallCount++;
        this.lastLoggedObject = object;
    }

    @Override
    public Response validateObject(String objectId, int transactionId, Action action) {
        this.validateObjectCallCount++;
        this.lastValidatedObjectId = objectId;
        this.lastValidatedAction = action;
        this.lastValidatedTransactionId = transactionId;
        
        return new Response(this.shouldAllowValidation, this.validationMessage);
    }

    @Override
    public Response validateObjects(List<String> objectIds, int transactionId, Action action) {
        this.validateObjectCallCount++; // Hitung sebagai panggilan validasi
        this.lastValidatedTransactionId = transactionId;
        this.lastValidatedAction = action;
        
        if (objectIds != null && !objectIds.isEmpty()) {
            this.lastValidatedObjectId = objectIds.get(0); // Simpan item pertama sebagai representasi
        }
        
        return new Response(this.shouldAllowValidation, this.validationMessage);
    }

    @Override
    public void endTransaction(int transactionId, boolean commit) {
        this.endTransactionCallCount++;
        this.lastEndedTransactionId = transactionId;
        this.lastEndCommitStatus = commit;
    }

    // --- Helper Methods untuk Verifikasi Test ---

    public int getBeginTransactionCallCount() {
        return beginTransactionCallCount;
    }

    public int getEndTransactionCallCount() {
        return endTransactionCallCount;
    }

    public Integer getLastEndedTransactionId() {
        return lastEndedTransactionId;
    }

    public Boolean getLastEndCommitStatus() {
        return lastEndCommitStatus;
    }

    public int getValidateObjectCallCount() {
        return validateObjectCallCount;
    }

    public String getLastValidatedObjectId() {
        return lastValidatedObjectId;
    }

    public Action getLastValidatedAction() {
        return lastValidatedAction;
    }

    public int getLogObjectCallCount() {
        return logObjectCallCount;
    }

    public Row getLastLoggedObject() {
        return lastLoggedObject;
    }

    public void reset() {
        this.beginTransactionCallCount = 0;
        this.endTransactionCallCount = 0;
        this.validateObjectCallCount = 0;
        this.logObjectCallCount = 0;
        
        this.lastEndedTransactionId = null;
        this.lastEndCommitStatus = null;
        this.lastValidatedObjectId = null;
        this.lastValidatedAction = null;
        this.lastLoggedObject = null;
        
        this.shouldAllowValidation = true;
    }
}