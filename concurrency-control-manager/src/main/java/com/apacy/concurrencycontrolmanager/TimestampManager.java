package com.apacy.concurrencycontrolmanager; 
 
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
 
public class TimestampManager { 

    private final AtomicLong timeStamp;
    private final ConcurrentHashMap<String, Long> readTime;
    private final ConcurrentHashMap<String, Long> writeTime;
 
    public TimestampManager() { 
        this.timeStamp = new AtomicLong(0);
        this.readTime = new ConcurrentHashMap<>();
        this.writeTime = new ConcurrentHashMap<>();
    } 
 
    public long generateTimestamp() { 
        return timeStamp.getAndIncrement();
    } 
 
    public long getReadTimestamp(String dataItem) { 
        return readTime.getOrDefault(dataItem, 0L);
    } 
 
    public long getWriteTimestamp(String dataItem) { 
        return writeTime.getOrDefault(dataItem, 0L);
    } 
 
    public void updateReadTimestamp(String dataItem, long timestamp) { 
        readTime.put(dataItem, Math.max(getReadTimestamp(dataItem), timestamp));
    } 
 
    public void updateWriteTimestamp(String dataItem, long timestamp) { 
        writeTime.put(dataItem, Math.max(getWriteTimestamp(dataItem), timestamp));
    } 
    
    public boolean isReadValid(String dataItem, long transactionTimestamp) { 
        long wt = getWriteTimestamp(dataItem);
        return transactionTimestamp >= wt;
    } 
 
    public boolean isWriteValid(String dataItem, long transactionTimestamp) { 
        long rt = getReadTimestamp(dataItem);
        long wt = getWriteTimestamp(dataItem);

        return transactionTimestamp >= rt && transactionTimestamp >= wt;
    } 
} 