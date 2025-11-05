package com.apacy.concurrencycontrol;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IConcurrencyControlManager;

public class ConcurrencyControlManager extends DBMSComponent implements IConcurrencyControlManager {

    public ConcurrencyControlManager() {
        super("Concurrency Control Manager");
    }

    @Override
    public void initialize() throws Exception {
        // TODO: Initialize component
    }

    @Override
    public void shutdown() {
        // TODO: Shutdown component
    }

    @Override
    public ExecutionResult beginTransaction(String transactionId) {
        throw new UnsupportedOperationException("beginTransaction not implemented yet");
    }

    @Override
    public ExecutionResult commitTransaction(String transactionId) {
        throw new UnsupportedOperationException("commitTransaction not implemented yet");
    }

    @Override
    public ExecutionResult rollbackTransaction(String transactionId) {
        throw new UnsupportedOperationException("rollbackTransaction not implemented yet");
    }

    @Override
    public ExecutionResult requestReadLock(DataRetrieval retrieval) {
        throw new UnsupportedOperationException("requestReadLock not implemented yet");
    }

    @Override
    public ExecutionResult requestWriteLock(DataWrite write) {
        throw new UnsupportedOperationException("requestWriteLock not implemented yet");
    }

    @Override
    public ExecutionResult requestWriteLock(DataDeletion deletion) {
        throw new UnsupportedOperationException("requestWriteLock not implemented yet");
    }

    @Override
    public ExecutionResult releaseLocks(String transactionId) {
        throw new UnsupportedOperationException("releaseLocks not implemented yet");
    }

    @Override
    public ExecutionResult validateTransaction(String transactionId) {
        throw new UnsupportedOperationException("validateTransaction not implemented yet");
    }

    @Override
    public ExecutionResult detectDeadlock() {
        throw new UnsupportedOperationException("detectDeadlock not implemented yet");
    }

    @Override
    public ExecutionResult getTransactionStatus(String transactionId) {
        throw new UnsupportedOperationException("getTransactionStatus not implemented yet");
    }
}
