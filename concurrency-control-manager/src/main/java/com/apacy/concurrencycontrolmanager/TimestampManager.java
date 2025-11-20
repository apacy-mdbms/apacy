package com.apacy.concurrencycontrolmanager;

import java.util.HashMap;
import java.util.Map;

public class TimestampManager {

    private long timeStamp;
    
    private final Map<String, Long> readTime;
    private final Map<String, Long> writeTime;

    public TimestampManager() {
        this.timeStamp = 0;
        this.readTime = new HashMap<>();
        this.writeTime = new HashMap<>();
    }

    // PENTING: synchronized karena 'timeStamp++' bukan operasi atomik
    public synchronized long generateTimestamp() {
        return timeStamp++;
    }

    public synchronized long getReadTimestamp(String dataItem) {
        return readTime.getOrDefault(dataItem, 0L);
    }

    public synchronized long getWriteTimestamp(String dataItem) {
        return writeTime.getOrDefault(dataItem, 0L);
    }

    public synchronized void updateReadTimestamp(String dataItem, long timestamp) {
        readTime.put(dataItem, Math.max(getReadTimestamp(dataItem), timestamp));
    }

    public synchronized void updateWriteTimestamp(String dataItem, long timestamp) {
        writeTime.put(dataItem, Math.max(getWriteTimestamp(dataItem), timestamp));
    }

    public synchronized boolean isReadValid(String dataItem, long transactionTimestamp) {
        long wt = getWriteTimestamp(dataItem);
        return transactionTimestamp >= wt;
    }

    public synchronized boolean isWriteValid(String dataItem, long transactionTimestamp) {
        long rt = getReadTimestamp(dataItem);
        long wt = getWriteTimestamp(dataItem);

        return transactionTimestamp >= rt && transactionTimestamp >= wt;
    }
}