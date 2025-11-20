package com.apacy.queryprocessor;

import java.io.IOException;
import java.util.List;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Response;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.ddl.ParsedQueryCreate;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IQueryOptimizer;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.queryoptimizer.ast.expression.ColumnFactor;
import com.apacy.queryoptimizer.ast.join.JoinConditionNode;
import com.apacy.queryoptimizer.ast.join.JoinOperand;
import com.apacy.queryoptimizer.ast.join.TableNode;
import com.apacy.queryoptimizer.ast.where.ComparisonConditionNode;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;
import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.execution.SortStrategy;
import com.apacy.queryprocessor.mocks.MockDDLParser;

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
        MockDDLParser ddlParser = new MockDDLParser(sqlQuery);
        if (ddlParser.isDDL()) {
            return executeDDL(ddlParser);
        }

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


    /**
     * Handler khusus untuk DDL (Create, Drop) pake Mock Parser.
     */
    private ExecutionResult executeDDL(MockDDLParser parser) {
        try {
            ParsedQueryDDL ddlQuery = parser.parseDDL();

            // CREATE TABLE
            if (ddlQuery instanceof ParsedQueryCreate createCmd) {
                Schema schema = planTranslator.translateToSchema(createCmd);
                
                sm.createTable(schema);
                
                return new ExecutionResult(true, "Table '" + createCmd.getTableName() + "' created successfully.", 0, "CREATE", 0, null);
            }

           // TODO: DROP TABLE HANDLER (BLOCKER: dropTable method from SM)

            return new ExecutionResult(false, "Unknown DDL Command", 0, "DDL", 0, null);

        } catch (IOException e) {
            return new ExecutionResult(false, "Storage IO Error: " + e.getMessage(), 0, "DDL", 0, null);
        } catch (RuntimeException e) {
            return new ExecutionResult(false, "Parsing/Logic Error: " + e.getMessage(), 0, "DDL", 0, null);
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

    // --- Main Execution Logic ---
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
        
        // 3. Sorting (ORDER BY)
        if (query.orderByColumn() != null) {
            boolean ascending = !query.isDescending();
            rows = SortStrategy.sort(rows, query.orderByColumn(), ascending);
        }

        // 4. Commit & Return
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