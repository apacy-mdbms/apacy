package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockConcurrencyControlManager implements IConcurrencyControlManager {

    private boolean forceFail = false;
    private AtomicInteger counter = new AtomicInteger(0);
    
    @Override
    public int beginTransaction() {
        int txId = counter.incrementAndGet();
        System.out.println("[MOCK-CCM] beginTransaction: " + txId);
        return txId;
    }

    @Override
    public Response validateObject(String objectId, int transactionId, Action action) {
        if (forceFail) {
            return new Response(false, "Mock FAILED by force");
        }
        // System.out.println("[MOCK-CCM] validateObject SUKSES untuk: " + objectId + " (Tx:" + transactionId + ")");
        return new Response(true, "Mock OK");
    }

    @Override
    public Response validateObjects(List<String> objectIds, int transactionId, Action action) {
        for (String objectId : objectIds) {
            Response response = validateObject(objectId, transactionId, action);
            if (!response.isAllowed()) {
                return response;
            }
        }
        return new Response(true, "All locks acquired");
    }
    
    @Override
    public void endTransaction(int transactionId, boolean commit) {
        String status = commit ? "COMMIT" : "ABORT";
        System.out.println("[MOCK-CCM] endTransaction " + transactionId + ": " + status);
    }

    @Override
    public void logObject(Row object, int transactionId) {
        // Silent log
    }

    public void setForceFail(boolean forceFail) {
        this.forceFail = forceFail;
    }
}