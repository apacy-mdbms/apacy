package com.apacy.queryprocessor;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.*;
import com.apacy.common.enums.Action;

import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.execution.SortStrategy;

import com.apacy.queryprocessor.evaluator.ConditionEvaluator;

import com.apacy.queryoptimizer.ast.join.*;
import com.apacy.queryoptimizer.ast.where.*;
import com.apacy.queryoptimizer.ast.expression.*;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

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

    // --- Helper Methods for Join Execution ---

    private List<Row> evaluateJoinTree(JoinOperand operand) {
        if (operand instanceof TableNode tableNode) {
            String tableName = tableNode.tableName();
            DataRetrieval req = new DataRetrieval(tableName, List.of("*"), null, false);
            return sm.readBlock(req);
        }
        // Recursive Case: Join Node
        else if (operand instanceof JoinConditionNode joinNode) {
            List<Row> leftRows = evaluateJoinTree(joinNode.left());
            List<Row> rightRows = evaluateJoinTree(joinNode.right());

            String joinCol = extractJoinColumn(joinNode.conditions());

            return JoinStrategy.nestedLoopJoin(leftRows, rightRows, joinCol);
        } 
        
        throw new IllegalArgumentException("Unknown JoinOperand type: " + operand.getClass().getSimpleName());
    }

    private String extractJoinColumn(WhereConditionNode condition) {
        if (condition instanceof ComparisonConditionNode comp) {
            if (comp.leftOperand().term().factor() instanceof ColumnFactor col) {
                String fullName = col.columnName();
                // Jika format "table.column", ambil "column"-nya saja
                return fullName.contains(".") ? fullName.split("\\.")[1] : fullName;
            }
        }
        // Fallback
        return "id";
    }

    private ExecutionResult executeSelect(ParsedQuery query, int txId) throws Exception {
        List<String> targetTables = query.targetTables();
        
        if (targetTables != null && !targetTables.isEmpty()) {
            List<String> objectIds = targetTables.stream()
                .map(tableName -> "TABLE::" + tableName)
                .toList();

            Response res = ccm.validateObjects(objectIds, txId, Action.READ);
            
            if (!res.isAllowed()) { 
                throw new Exception("Lock denied: " + res.reason()); 
            }
        }

        List<Row> rows;

        // 2. Execution Strategy (Join vs Single Table)
        if (query.joinConditions() != null && query.joinConditions() instanceof JoinOperand) {
            rows = evaluateJoinTree((JoinOperand) query.joinConditions());
        } else {
            DataRetrieval dataRetrieval = planTranslator.translateToRetrieval(query, String.valueOf(txId));
            rows = sm.readBlock(dataRetrieval);
        }

        // 3. Filtering (WHERE Clause)
        if (query.whereClause() != null) {
            rows = rows.stream()
                .filter(row -> ConditionEvaluator.evaluate(row, query.whereClause()))
                .collect(Collectors.toList());
        }

        // 4. Projection (SELECT col1, col2...)
        // Kalau bukan "SELECT *", ambil kolom yang diminta doang
        if (query.targetColumns() != null && !query.targetColumns().contains("*")) {
            final List<String> cols = query.targetColumns();
            rows = rows.stream().map(row -> {
                Map<String, Object> projectedData = new HashMap<>();
                for (String colName : cols) {
                    Object val = getColumnValue(row, colName);
                    projectedData.put(colName, val);
                }
                return new Row(projectedData);
            }).collect(Collectors.toList());
        }
        
        // 5. Sorting (ORDER BY)
        if (query.orderByColumn() != null) {
            boolean ascending = !query.isDescending();
            rows = SortStrategy.sort(rows, query.orderByColumn(), ascending);
        }

        // LIMIT & OFFSET di sini
        
        // 6. Commit & Return
        ccm.endTransaction(txId, true);
        
        return new ExecutionResult(
            true, 
            "SELECT executed successfully", 
            txId, 
            "SELECT", 
            rows.size(), 
            rows
        );
    }

    private Object getColumnValue(Row row, String requestedCol) {
        // 1. Cek exact match (misal: query minta "id", di row ada "id")
        if (row.data().containsKey(requestedCol)) {
            return row.data().get(requestedCol);
        }
        
        // 2. Cek suffix match (misal: query minta "id", di row ada "users.id")
        for (String key : row.data().keySet()) {
            if (key.endsWith("." + requestedCol)) {
                return row.data().get(key);
            }
        }
        
        // 3. Cek jika query minta "users.id" tapi di row cuma ada "id"
        if (requestedCol.contains(".")) {
            String simpleName = requestedCol.substring(requestedCol.lastIndexOf(".") + 1);
            if (row.data().containsKey(simpleName)) {
                return row.data().get(simpleName);
            }
        }
        
        return null;
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