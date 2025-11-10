package com.apacy.concurrencycontrolmanager;

import com.apacy.common.dto.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Internal Transaction object for tracking transaction status.
 */
public class Transaction {

    public enum TransactionStatus {
        ACTIVE, PARTIALLY_COMMITTED, COMMITTED, FAILED, ABORTED, TERMINATED
    }

    private final String transactionId;
    private TransactionStatus status;
    private long timestamp;

    private final List<Row> loggedObjects = Collections.synchronizedList(new ArrayList<>());

    public Transaction(String transactionId) {
        this.transactionId = transactionId;
        this.status = TransactionStatus.ACTIVE;
        this.timestamp = 0L;
    }

    /**
     * Get the transaction ID.
     */
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Get the current transaction status.
     */
    public TransactionStatus getStatus() {
        return status;
    }

    /**
     * Get the transaction timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        if (this.timestamp != 0L)
            throw new IllegalStateException("Timestamp already set for this transaction.");
        this.timestamp = timestamp;
    }

    /**
     * Check if transaction is active.
     */
    public boolean isActive() {
        return status == TransactionStatus.ACTIVE;
    }

    public boolean isFailed() {
        return status == TransactionStatus.FAILED;
    }

    public boolean isPartiallyCommitted() {
        return status == TransactionStatus.PARTIALLY_COMMITTED;
    }

    /**
     * Check if transaction is committed.
     */
    public boolean isCommitted() {
        return status == TransactionStatus.COMMITTED;
    }

    /**
     * Check if transaction is aborted.
     */
    public boolean isAborted() {
        return status == TransactionStatus.ABORTED;
    }

    public boolean isTerminated() {
        return status == TransactionStatus.TERMINATED;
    }

    public void setPartiallyCommitted() {
        if (status != TransactionStatus.ACTIVE) {
            throw new IllegalStateException("Can only move to PARTIALLY_COMMITTED from ACTIVE.");
        }
        updateStatus(TransactionStatus.PARTIALLY_COMMITTED);
    }

    public void commit() {
        if (status != TransactionStatus.PARTIALLY_COMMITTED) {
            throw new IllegalStateException("Can only commit from PARTIALLY_COMMITTED state.");
        }
        updateStatus(TransactionStatus.COMMITTED);
    }

    public void setFailed() {
        if (status != TransactionStatus.ACTIVE && status != TransactionStatus.PARTIALLY_COMMITTED) {
            throw new IllegalStateException("Can only set FAILED from ACTIVE or PARTIALLY_COMMITTED.");
        }
        updateStatus(TransactionStatus.FAILED);
    }

    public void abort() {
        if (status != TransactionStatus.FAILED) {
            throw new IllegalStateException("Can only abort from FAILED state.");
        }
        updateStatus(TransactionStatus.ABORTED);
    }

    public void terminate() {
        if (status != TransactionStatus.COMMITTED && status != TransactionStatus.ABORTED) {
            throw new IllegalStateException("Can only terminate from COMMITTED or ABORTED.");
        }
        updateStatus(TransactionStatus.TERMINATED);
    }

    private void updateStatus(TransactionStatus newStatus) {
        TransactionStatus oldStatus = this.status;
        this.status = newStatus;
        log("Status changed: " + oldStatus + " to " + newStatus);
    }

    private void log(String message) {
        System.out.println("[Transaction " + transactionId + "] " + message);
    }

    public void addLog(Row row) {
        if (row != null) {
            loggedObjects.add(row);
            System.out.println("[Transaction " + transactionId + "] Logged object: " + row);
        }
    }

    public List<Row> getLoggedObjects() {
        synchronized (loggedObjects) {
            return new ArrayList<>(loggedObjects);
        }
    }

    @Override
    public String toString() {
        return String.format("Transaction{id='%s', status=%s, timestamp=%d}",
                transactionId, status, timestamp);
    }
}