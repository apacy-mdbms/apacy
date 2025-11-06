package com.apacy.queryprocessor;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.*;

import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.execution.SortStrategy;

/**
 * Main Query Processor that coordinates all database operations.
 * TODO: Implement query processing pipeline with parsing, optimization, and execution coordination
 */
public class QueryProcessor extends DBMSComponent {
    private final IQueryOptimizer qo;
    private final IStorageManager sm;
    private final IConcurrencyControlManager ccm;
    private final IFailureRecoveryManager frm;

    private final PlanTranslator planTranslator;
    private final JoinStrategy joinStrategy;
    private final SortStrategy sortStrategy;
    
    private boolean initialized = false;

    public QueryProcessor(
        IQueryOptimizer qo, 
        IStorageManager sm,
        IConcurrencyControlManager ccm,
        IFailureRecoveryManager frm
        ) {
        super("Query Processor");
        this.qo = qo;
        this.sm = sm;
        this.ccm = ccm;
        this.frm = frm;

        this.planTranslator = new PlanTranslator();
        this.joinStrategy = new JoinStrategy();
        this.sortStrategy = new SortStrategy();
    }
    
    /**
     * Initialize the query processor and all its components.
     */
    @Override
    public void initialize() throws Exception {
        this.initialized = true;
        System.out.println("Query Processor has been initialized.");
    }
    
    /**
     * Shutdown the query processor and all its components.
     * TODO: Implement graceful shutdown of all components with proper resource cleanup
     */
    @Override
    public void shutdown() {
        this.initialized = false;
        System.out.println("Query Processor has been shutdown.");
    }
    
    /**
     * Execute a SQL query and return the result.
     * TODO: Implement complete query execution pipeline with parsing, optimization, and execution
     */
    public ExecutionResult executeQuery(String sqlQuery) {
        // TODO: Implement query execution pipeline
        throw new UnsupportedOperationException("executeQuery not implemented yet");
    }
}
