package com.apacy.queryprocessor;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.*;
import com.apacy.common.enums.Action;

import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.execution.SortStrategy;

import java.util.List;


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
        ParsedQuery parsedQuery = qo.optimizeQuery(qo.parseQuery(sqlQuery), sm.getAllStats());
        int txId = ccm.beginTransaction();
        try {
            switch (parsedQuery.queryType()){
                case "SELECT" :

                    // maaf gue implementasi tipis buat testing
                    DataRetrieval dataRetrieval = 
                        planTranslator.translateToRetrieval(parsedQuery, String.valueOf(txId));
                    String objectId = "TABLE::" + dataRetrieval.tableName();

                    Response res = ccm.validateObject(objectId, txId, Action.READ);

                    if (!res.isAllowed()) { throw new Exception(res.reason()); }

                    List<Row> rows = sm.readBlock(dataRetrieval);

                    ccm.endTransaction(txId, true);
                    
                    ExecutionResult result = new ExecutionResult(
                        true, 
                        "SELECT executed successfully", 
                        txId, 
                        "SELECT", 
                        rows.size(), // affectedRows adalah jumlah row yang diambil
                        rows         // data List<Row>
                    );
                    
                    frm.writeLog(result);
                    
                    return result;

                default:
                    throw new UnsupportedOperationException("Query type '" + parsedQuery.queryType() + "' not supported yet.");             
            }

        } catch (Exception e) {
            ccm.endTransaction(txId, false);
            frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", String.valueOf(txId), null));

            return new ExecutionResult(false, e.getMessage(), txId, parsedQuery.queryType(), 0, null);
        }
    }
}
