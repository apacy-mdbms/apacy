package com.apacy.concurrencycontrolmanager;

import com.apacy.common.interfaces.IConcurrencyControlManager;

public interface IConcurrencyControlManagerAlgorithm extends IConcurrencyControlManager{
    /**
     * Initialize the component.
     * @throws Exception if initialization fails
     */
    void initialize() throws Exception;
    
    /**
     * Shutdown the component gracefully.
     */
    void shutdown();
} 
