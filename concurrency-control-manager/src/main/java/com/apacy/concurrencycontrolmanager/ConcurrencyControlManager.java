package com.apacy.concurrencycontrolmanager;

import java.util.List;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.Response;
import com.apacy.common.dto.Row;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;

public class ConcurrencyControlManager extends DBMSComponent implements IConcurrencyControlManager {
    
    private IConcurrencyControlManagerAlgorithm manager;
 
    private final IFailureRecoveryManager failureRecoveryManager;

    public ConcurrencyControlManager() {
        this("lock", null);
    }

    public ConcurrencyControlManager(String algorithm) {
        this(algorithm, null);
        this.manager = switch(algorithm) {
            case "lock" -> new ConcurrencyControlManagerLockBased(failureRecoveryManager);
            case "timestamp" -> new ConcurrencyControlManagerTimestamp(failureRecoveryManager);
            case "validation" -> new ConcurrencyControlManagerValidation(failureRecoveryManager);
            default -> null;
        };
        if (this.manager == null) {
            throw new IllegalArgumentException("Invalid Algorithm: " + algorithm);
        }
    }

    public ConcurrencyControlManager(String algorithm, IFailureRecoveryManager failureRecoveryManager) {
        super("Concurrency Control Manager");
        this.failureRecoveryManager = failureRecoveryManager;
        this.manager = switch(algorithm) {
            case "lock" -> new ConcurrencyControlManagerLockBased(failureRecoveryManager);
            case "timestamp" -> new ConcurrencyControlManagerTimestamp(failureRecoveryManager);
            case "validation" -> new ConcurrencyControlManagerValidation(failureRecoveryManager);
            default -> null;
        };
        if (this.manager == null) {
            throw new IllegalArgumentException("Invalid Algorithm: " + algorithm);
        }
    }

    @Override
    public synchronized void initialize() throws Exception {
        this.manager.initialize();
    }

    @Override
    public synchronized void shutdown() {
        this.manager.shutdown();
    }

    @Override
    public synchronized int beginTransaction() {
        return this.manager.beginTransaction();
    }

    @Override
    public synchronized Response validateObject(String objectId, int transactionId, Action action) {
        return this.manager.validateObject(objectId, transactionId, action);
    }

    @Override
    public synchronized Response validateObjects(List<String> objectIds, int transactionId, Action action) {
        return this.manager.validateObjects(objectIds, transactionId, action);
    }


    @Override
    public synchronized void endTransaction(int transactionId, boolean commit) {
        this.manager.endTransaction(transactionId, commit);
    }

    @Override
    public synchronized void logObject(Row object, int transactionId) {
        this.manager.logObject(object, transactionId);
    }
}
