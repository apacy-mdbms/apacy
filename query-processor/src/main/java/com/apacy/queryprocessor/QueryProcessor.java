package com.apacy.queryprocessor;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.*;
import com.apacy.common.enums.Action;

import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.execution.SortStrategy;

import com.apacy.queryoptimizer.ast.join.*;
import com.apacy.queryoptimizer.ast.where.*;
import com.apacy.queryoptimizer.ast.expression.*;

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
        // 1. Parsing & Optimization
        ParsedQuery parsedQuery = qo.optimizeQuery(qo.parseQuery(sqlQuery), sm.getAllStats());
        int txId = ccm.beginTransaction();
        
        try {
            switch (parsedQuery.queryType()){
                case "SELECT":
                    List<Row> rows;

                    if (parsedQuery.joinConditions() != null) {
                        rows = evaluateJoinTree((JoinOperand) parsedQuery.joinConditions());
                    } else {
                        DataRetrieval retrieval = planTranslator.translateToRetrieval(parsedQuery, String.valueOf(txId));
                        
                        String objectId = "TABLE::" + retrieval.tableName();
                        Response res = ccm.validateObject(objectId, txId, Action.READ);
                        if (!res.isAllowed()) { 
                            throw new Exception("Lock denied: " + res.reason()); 
                        }

                        rows = sm.readBlock(retrieval);
                    }

                    if (parsedQuery.orderByColumn() != null) {
                        boolean ascending = !parsedQuery.isDescending();
                        rows = SortStrategy.sort(rows, parsedQuery.orderByColumn(), ascending);
                    }

                    ccm.endTransaction(txId, true);
                    
                    ExecutionResult result = new ExecutionResult(
                        true, 
                        "SELECT executed successfully", 
                        txId, 
                        "SELECT", 
                        rows.size(),
                        rows
                    );
                    
                    try {
                        frm.writeLog(result);
                    } catch (Exception e) {
                        System.err.println("[WARNING] Log failed: " + e.getMessage());
                    }
                    
                    return result;

                default:
                    throw new UnsupportedOperationException("Query type '" + parsedQuery.queryType() + "' not supported yet.");             
            }

        } catch (Exception e) {
            try {
                ccm.endTransaction(txId, false);
                frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", String.valueOf(txId), null));
            } catch (Exception ex) {

            }
            return new ExecutionResult(false, e.getMessage(), txId, parsedQuery.queryType(), 0, null);
        }
    }

    private List<Row> evaluateJoinTree(JoinOperand operand) {
        // Base
        if (operand instanceof TableNode tableNode) {
            String tableName = tableNode.tableName();
            DataRetrieval req = new DataRetrieval(tableName, List.of("*"), null, false);
            return sm.readBlock(req);
        }
        // Recursion
        else if (operand instanceof JoinConditionNode joinNode) {
            List<Row> leftRows = evaluateJoinTree(joinNode.left());
            List<Row> rightRows = evaluateJoinTree(joinNode.right());

            String joinCol = extractJoinColumn(joinNode.conditions());

            return JoinStrategy.nestedLoopJoin(leftRows, rightRows, joinCol);
        } 
        throw new IllegalArgumentException("Unknown JoinOperand type");
    }

    private String extractJoinColumn(WhereConditionNode condition) {
        if (condition instanceof ComparisonConditionNode comp) {
            if (comp.leftOperand().term().factor() instanceof ColumnFactor col) {
                String fullName = col.columnName();
                return fullName.contains(".") ? fullName.split("\\.")[1] : fullName;
            }
        }
        return "id";
    }
}