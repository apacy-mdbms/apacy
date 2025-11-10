package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;

public class MockConcurrencyControlManager implements IConcurrencyControlManager {

    private boolean forceFail = false;
    private int transactionIdCounter = 0;
    
    @Override
    public int beginTransaction() {
        int txId = ++transactionIdCounter;
        System.out.println("[MOCK-CCM] beginTransaction dipanggil, mengembalikan txId: " + txId);
        return txId;
    }

    @Override
    public Response validateObject(String objectId, int transactionId, Action action) {
        if (forceFail) {
            return new Response(false, "Mock FAILED by force");
        }
        
        System.out.println("[MOCK-CCM] validateObject SUKSES untuk: " + objectId + 
                          " (txId: " + transactionId + ", action: " + action + ")");
        return new Response(true, "Mock validation OK");
    }
    
    @Override
    public void endTransaction(int transactionId, boolean commit) {
        String status = commit ? "COMMIT" : "ABORT";
        System.out.println("[MOCK-CCM] endTransaction dipanggil untuk txId " + transactionId + ": " + status);
    }

    @Override
    public void logObject(Row object, int transactionId) {
        System.out.println("[MOCK-CCM] logObject dipanggil untuk txId: " + transactionId + 
                          " dengan row data: " + object.data().size() + " kolom");
    }

    public void setForceFail(boolean forceFail) {
        this.forceFail = forceFail;
    }
}