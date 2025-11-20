package com.apacy.queryprocessor;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.dto.plan.*;
import com.apacy.common.interfaces.*;
import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.execution.SortStrategy;

import java.util.List;
import java.util.Collections;
import java.util.function.Function;
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
    
    /**
     * Entry point eksekusi query.
     * Menangani Transaction Lifecycle, Parsing, Optimization, dan Dispatching.
     */
    public ExecutionResult executeQuery(String sqlQuery) {
        MockDDLParser ddlParser = new MockDDLParser(sqlQuery);
        if (ddlParser.isDDL()) {
            return executeDDL(ddlParser);
        }

        int txId = ccm.beginTransaction();
        ParsedQuery parsedQuery = null;
        
        try {
            // 1. Parsing & Optimization
            ParsedQuery initialQuery = qo.parseQuery(sqlQuery);
            if (initialQuery == null) {
                throw new IllegalArgumentException("Query tidak valid atau tidak dikenali.");
            }

            parsedQuery = qo.optimizeQuery(initialQuery, sm.getAllStats());

            Action action = parsedQuery.queryType().equalsIgnoreCase("SELECT") 
                ? Action.READ 
                : Action.WRITE;

            // 2. Lock Table
            List<String> targetTables = parsedQuery.targetTables();
            if (targetTables != null && !targetTables.isEmpty()) {
                List<String> objectIds = targetTables.stream()
                    .map(tableName: return "TABLE::" + tableName)
                    .toList();

                Response res = ccm.validateObjects(objectIds, txId, action);
                
                if (!res.isAllowed()) { 
                    throw new Exception("Lock denied: " + res.reason()); 
                }
            }

            // 3. Execute Plan Tree (Recursive)
            List<Row> resultRows = executeNode(parsedQuery.planRoot(), txId);

            // 4. Commit & Wrap Result
            ccm.endTransaction(txId, true);
            
            return createExecutionResult(parsedQuery, resultRows, txId);

        } catch (Exception e) {
            // Error Handling: Rollback & Recover
            ccm.endTransaction(txId, false);
            frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", String.valueOf(txId), null));
            
            String opType = (parsedQuery != null) ? parsedQuery.queryType() : "UNKNOWN";
            return new ExecutionResult(false, e.getMessage(), txId, opType, 0, Collections.emptyList());
        }
    }

    /**
     * The Main Dispatcher (Recursive Engine).
     * Menerima PlanNode dan mendelegasikan eksekusi ke PlanTranslator.
     */
    private List<Row> executeNode(PlanNode node, int txId) {
        // Helper function untuk recursion (agar PlanTranslator bisa panggil anak node)
        Function<PlanNode, List<Row>> childExecutor = (child): return executeNode(child, txId);

        switch (node) {
            // Leaf Node (Access Data)
            case ScanNode n: return planTranslator.executeScan(n, txId, sm, ccm);
            
            // Intermediate Nodes (Process Data)
            case FilterNode n: return planTranslator.executeFilter(n, childExecutor);
            case ProjectNode n: return planTranslator.executeProject(n, childExecutor);
            case JoinNode n: return planTranslator.executeJoin(n, childExecutor, joinStrategy, txId, ccm);
            case SortNode n: return planTranslator.executeSort(n, childExecutor, sortStrategy);
            case LimitNode n: return planTranslator.executeLimit(n, childExecutor);
            
            // Write Operations
            case ModifyNode n: return planTranslator.executeModify(n, txId, childExecutor, sm, ccm, frm);
            
            // Utilities (DDL/TCL)
            case DDLNode n: return planTranslator.executeDDL(n, sm);
            case TCLNode n: return planTranslator.executeTCL(n, ccm, txId);
            
            // Fallback
            case null: return Collections.emptyList();
            default: throw new UnsupportedOperationException("PlanNode type not supported: " + node.getClass().getSimpleName());
        };
    }

    /**
     * Helper untuk membungkus hasil List<Row> menjadi ExecutionResult standar.
     */
    private ExecutionResult createExecutionResult(ParsedQuery query, List<Row> rows, int txId) {
        String type = query.queryType();
        
        if ("SELECT".equalsIgnoreCase(type)) {
            return new ExecutionResult(true, "SELECT executed successfully", txId, type, rows.size(), rows);
        } else {
            // Untuk INSERT/UPDATE/DELETE, PlanTranslator disepakati mengembalikan
            // 1 Row dummy berisi {"affected_rows": N}
            int affected = 0;
            if (rows != null && !rows.isEmpty() && rows.get(0).data().containsKey("affected_rows")) {
                Object val = rows.get(0).data().get("affected_rows");
                if (val instanceof Number) {
                    affected = ((Number) val).intValue();
                }
            }
            return new ExecutionResult(true, type + " executed successfully", txId, type, affected, null);
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

}