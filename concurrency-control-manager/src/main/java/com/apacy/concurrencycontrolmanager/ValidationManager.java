package com.apacy.concurrencycontrolmanager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Validation based (OCC) protocol
 */
public class ValidationManager {

    // menyimpan state internal per transaksi
    private static class TransactionState {
        long startTS;
        long validationTS;
        long finishTS;
        final Set<String> readSet = new HashSet<>();
        final Set<String> writeSet = new HashSet<>();
    }

    private long timeStamp = 0L;
    private final Map<Integer, TransactionState> transactionStates = new HashMap<>();

    // menyimpan transaksi yang bisa commit
    private final List<Integer> committed = new ArrayList<>();

    private synchronized long generateTimestamp() {
        return ++timeStamp;
    }

    public synchronized void onTransactionStart(int txId) {
        TransactionState ts = new TransactionState();
        ts.startTS = generateTimestamp();
        transactionStates.put(txId, ts);
    }

    /** read set */
    public synchronized void recordRead(int txId, String objectId) {
        TransactionState ts = transactionStates.get(txId);
        if (ts == null) {
            ts = new TransactionState();
            ts.startTS = generateTimestamp();
            transactionStates.put(txId, ts);
        }
        ts.readSet.add(objectId);
    }

    /** writeset */
    public synchronized void recordWrite(int txId, String objectId) {
        TransactionState ts = transactionStates.get(txId);
        if (ts == null) {
            ts = new TransactionState();
            ts.startTS = generateTimestamp();
            transactionStates.put(txId, ts);
        }
        ts.writeSet.add(objectId);
    }

    /** VALIDATION TEST */
    public synchronized boolean validate(int txId) {
        TransactionState tj = transactionStates.get(txId);
        if (tj == null) {
            return true;
        }

        tj.validationTS = generateTimestamp();

        for (Integer tiId : committed) {
            TransactionState ti = transactionStates.get(tiId);
            if (ti == null)
                continue;

            if (ti.validationTS >= tj.validationTS) {
                continue;
            }

            long finishTi = ti.finishTS;
            long startTj = tj.startTS;
            long validTj = tj.validationTS;

            boolean condition1 = (finishTi < startTj);

            boolean condition2 = false;
            if (startTj < finishTi && finishTi < validTj) {
                boolean disjoint = true; // tidak intersect
                for (String w : ti.writeSet) {
                    if (tj.readSet.contains(w)) {
                        disjoint = false;
                        break;
                    }
                }
                condition2 = disjoint;
            }

            if (condition1 || condition2) {
                continue;
            } else {
                return false;
            }
        }

        tj.finishTS = generateTimestamp();
        committed.add(txId);
        return true;
    }

    /** cleanup saat abort */
    public synchronized void onAbort(int txId) {
        transactionStates.remove(txId);
    }
}
