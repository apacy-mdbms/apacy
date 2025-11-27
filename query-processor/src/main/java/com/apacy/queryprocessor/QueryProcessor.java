package com.apacy.queryprocessor;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Response;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.dto.plan.CartesianNode;
import com.apacy.common.dto.plan.DDLNode;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.LimitNode;
import com.apacy.common.dto.plan.ModifyNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.dto.plan.SortNode;
import com.apacy.common.dto.plan.TCLNode;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IQueryOptimizer;
import com.apacy.common.interfaces.IStorageManager;
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
    private QueryBinder queryBinder;
    
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

        if (this.sm instanceof com.apacy.storagemanager.StorageManager concreteSM) {
            this.queryBinder = new QueryBinder(concreteSM.getCatalogManager());
            System.out.println("Query Binder initialized successfully.");
        } else {
            System.err.println("Warning: Could not initialize QueryBinder (StorageManager is not concrete implementation)");
        }

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

            ParsedQuery boundQuery = initialQuery;
            
            if (queryBinder != null) {
                System.out.println("\n--- [DEBUG QP] BEFORE BINDING ---");
                System.out.println("Columns: " + initialQuery.targetColumns());
                System.out.println("Where  : " + initialQuery.whereClause());
                
                // Lakukan Binding
                boundQuery = queryBinder.bind(initialQuery);
                
                System.out.println("--- [DEBUG QP] AFTER BINDING ---");
                System.out.println("Columns: " + boundQuery.targetColumns());
                System.out.println("Where  : " + boundQuery.whereClause());
                System.out.println("----------------------------------\n");
            }

            parsedQuery = qo.optimizeQuery(initialQuery, sm.getAllStats());

            Action action = parsedQuery.queryType().equalsIgnoreCase("SELECT") 
                ? Action.READ 
                : Action.WRITE;

            // 2. Lock Table
            List<String> targetTables = parsedQuery.targetTables();
            if (targetTables != null && !targetTables.isEmpty()) {
                List<String> objectIds = targetTables.stream()
                    .map(tableName -> "TABLE::" + tableName)
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
        Function<PlanNode, List<Row>> childExecutor = (child) -> executeNode(child, txId);

        if (node == null) {
            return Collections.emptyList();
        }

        if (node instanceof ScanNode n) {
            return planTranslator.executeScan(n, txId, sm, ccm);
        }
        if (node instanceof FilterNode n) {
            return planTranslator.executeFilter(n, childExecutor);
        }
        if (node instanceof ProjectNode n) {
            return planTranslator.executeProject(n, childExecutor);
        }
        if (node instanceof JoinNode n) {
            return planTranslator.executeJoin(n, childExecutor, joinStrategy, txId, ccm);
        }
        if (node instanceof SortNode n) {
            return planTranslator.executeSort(n, childExecutor, sortStrategy);
        }
        if (node instanceof LimitNode n) {
            return planTranslator.executeLimit(n, childExecutor);
        }
        if (node instanceof ModifyNode n) {
            return planTranslator.executeModify(n, txId, childExecutor, sm, ccm, frm);
        }
        if (node instanceof DDLNode n) {
            return planTranslator.executeDDL(n, sm);
        }
        if (node instanceof TCLNode n) {
            return planTranslator.executeTCL(n, ccm, txId);
        }
        if (node instanceof CartesianNode n) {
            return planTranslator.executeCartesian(n, childExecutor, txId, ccm);
        }

        throw new UnsupportedOperationException(
            "PlanNode type not supported: " + node.getClass().getSimpleName()
        );
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

            DDLNode ddlNode = new DDLNode(ddlQuery);

            planTranslator.executeDDL(ddlNode, sm);

            String opType = ddlQuery.getType().toString();
            String tableName = ddlQuery.getTableName();
            String msg = switch (ddlQuery.getType()) {
                case CREATE_TABLE -> "Table '" + tableName + "' created successfully.";
                case DROP_TABLE   -> "Table '" + tableName + "' dropped successfully.";
                case ALTER_TABLE  -> "Table '" + tableName + "' altered successfully.";
                default           -> "DDL Command executed.";
            };

            return new ExecutionResult(true, msg, 0, opType, 0, null);

        } catch (Exception e) {
            return new ExecutionResult(false, "DDL Execution Failed: " + e.getMessage(), 0, "DDL", 0, null);
        }
    }

}