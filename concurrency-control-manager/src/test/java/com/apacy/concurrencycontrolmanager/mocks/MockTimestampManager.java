package com.apacy.concurrencycontrolmanager.mocks;

import com.apacy.concurrencycontrolmanager.TimestampManager;

public class MockTimestampManager extends TimestampManager {
    private long counter = 0;

    public MockTimestampManager() {
        super();
    }

    @Override
    public synchronized long generateTimestamp() {
        return ++counter;
    }
}