package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;

import java.util.List;

public class MockConcurrencyControlManager implements IConcurrencyControlManager {

    private boolean forceFail = false;
    
    @Override
    public int beginTransaction() {
        return 1;
    }

    @Override
    public Response validateObject(String objectId, int transactionId, Action action) {
        if (forceFail) {
            return new Response(false, "Mock FAILED by force");
        }
        
        System.out.println("[MOCK-CCM] validateObject SUKSES untuk: " + objectId);
        return new Response(true, "Mock OK");
    }

    public Response validateObjects(List<String> objectIds, int transactionId, Action action) {
        for (String objectId : objectIds) {
            Response response = validateObject(objectId, transactionId, action);

            if (!response.isAllowed()) {
                return new Response(false,
                    "Failed on object '" + objectId + "': " + response.reason());
            }
        }

        // Tidak ada validateObject yang ggal
        return new Response(true, "All locks acquired for transaction " + transactionId);
    }
    
    @Override
    public void endTransaction(int transactionId, boolean commit) {
        String status = commit ? "COMMIT" : "ABORT";
        System.out.println("[MOCK-CCM] endTransaction dipanggil: " + status);
    }

    @Override
    public void logObject(Row object, int transactionId) {
        System.out.println("[MOCK-CCM] logObject dipanggil untuk txId: " + transactionId);
    }

    public void setForceFail(boolean forceFail) {
        this.forceFail = forceFail;
    }
}