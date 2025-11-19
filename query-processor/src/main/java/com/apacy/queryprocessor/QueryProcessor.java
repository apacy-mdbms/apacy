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
    
    @Override
    public void initialize() throws Exception {
        this.initialized = true;
        System.out.println("Query Processor has been initialized.");
    }
    
    @Override
    public void shutdown() {
        this.initialized = false;
        System.out.println("Query Processor has been shutdown.");
    }
    
    public ExecutionResult executeQuery(String sqlQuery) {
        int txId = ccm.beginTransaction();
        ParsedQuery parsedQuery = null;
        
        try {
            ParsedQuery initialQuery = qo.parseQuery(sqlQuery);
            
            if (initialQuery == null) {
                throw new IllegalArgumentException("Query tidak valid atau tidak dikenali.");
            }

            parsedQuery = qo.optimizeQuery(initialQuery, sm.getAllStats());

            switch (parsedQuery.queryType()) {
                case "SELECT":
                    return executeSelect(parsedQuery, txId);
                    
                case "INSERT":
                case "UPDATE":
                    return executeWrite(parsedQuery, txId);
                    
                case "DELETE":
                    return executeDelete(parsedQuery, txId);

                default:
                    throw new UnsupportedOperationException("Query type '" + parsedQuery.queryType() + "' not supported yet.");             
            }

        } catch (Exception e) {
            ccm.endTransaction(txId, false);
            frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", String.valueOf(txId), null));
            
            String opType = (parsedQuery != null) ? parsedQuery.queryType() : "UNKNOWN";

            return new ExecutionResult(false, e.getMessage(), txId, opType, 0, null);
        }
    }

    // Helper Methods

    private ExecutionResult executeSelect(ParsedQuery query, int txId) throws Exception {
        DataRetrieval dataRetrieval = planTranslator.translateToRetrieval(query, String.valueOf(txId));
        String objectId = "TABLE::" + dataRetrieval.tableName();

        // 1. Concurrency Control (Locking)
        Response res = ccm.validateObject(objectId, txId, Action.READ);
        if (!res.isAllowed()) { 
            throw new Exception("Lock denied: " + res.reason()); 
        }

        // 2. Storage Execution
        List<Row> rows = sm.readBlock(dataRetrieval);
        
        // 3. Sorting (jika ada ORDER BY)
        if (query.orderByColumn() != null) {
            boolean ascending = !query.isDescending();
            rows = SortStrategy.sort(rows, query.orderByColumn(), ascending);
        }

        // 4. Commit & Log
        ccm.endTransaction(txId, true);
        
        ExecutionResult result = new ExecutionResult(
            true, 
            "SELECT executed successfully", 
            txId, 
            "SELECT", 
            rows.size(), 
            rows
        );
        
        return result;
    }

    private ExecutionResult executeWrite(ParsedQuery query, int txId) throws Exception {
        boolean isUpdate = "UPDATE".equalsIgnoreCase(query.queryType());
        DataWrite dataWrite = planTranslator.translateToWrite(query, String.valueOf(txId), isUpdate);
        
        String objectId = "TABLE::" + dataWrite.tableName();

        // 1. Concurrency Control (Locking - WRITE)
        Response res = ccm.validateObject(objectId, txId, Action.WRITE);
        if (!res.isAllowed()) { 
            throw new Exception("Lock denied: " + res.reason()); 
        }

        // 2. Storage Execution
        int affectedRows = sm.writeBlock(dataWrite);

        // 3. Commit & Log
        ccm.endTransaction(txId, true);

        ExecutionResult result = new ExecutionResult(
            true, 
            query.queryType() + " executed successfully", 
            txId, 
            query.queryType(), 
            affectedRows, 
            null
        );

        frm.writeLog(result);
        return result;
    }

    private ExecutionResult executeDelete(ParsedQuery query, int txId) throws Exception {
        DataDeletion dataDeletion = planTranslator.translateToDeletion(query, String.valueOf(txId));
        String objectId = "TABLE::" + dataDeletion.tableName();

        // 1. Concurrency Control (Locking - WRITE)
        Response res = ccm.validateObject(objectId, txId, Action.WRITE);
        if (!res.isAllowed()) { 
            throw new Exception("Lock denied: " + res.reason()); 
        }

        // 2. Storage Execution
        int affectedRows = sm.deleteBlock(dataDeletion);

        // 3. Commit & Log
        ccm.endTransaction(txId, true);

        ExecutionResult result = new ExecutionResult(
            true, 
            "DELETE executed successfully", 
            txId, 
            "DELETE", 
            affectedRows, 
            null
        );

        frm.writeLog(result);
        return result;
    }
}