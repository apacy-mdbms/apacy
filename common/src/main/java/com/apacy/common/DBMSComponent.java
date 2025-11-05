package com.apacy.common;

/**
 * Base class for all DBMS components.
 * Provides common functionality and utilities shared across modules.
 */
public abstract class DBMSComponent {
    
    private final String componentName;
    
    public DBMSComponent(String componentName) {
        this.componentName = componentName;
    }
    
    public String getComponentName() {
        return componentName;
    }
    
    /**
     * Initialize the component.
     * @throws Exception if initialization fails
     */
    public abstract void initialize() throws Exception;
    
    /**
     * Shutdown the component gracefully.
     */
    public abstract void shutdown();
}
