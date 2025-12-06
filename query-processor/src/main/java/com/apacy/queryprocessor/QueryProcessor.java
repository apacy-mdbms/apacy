package com.apacy.queryprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.RecoveryCriteria;
import com.apacy.common.dto.Response;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.LiteralFactor;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.dto.plan.DDLNode;
import com.apacy.common.enums.Action;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IQueryOptimizer;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.queryprocessor.execution.Operator;
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
    }
    
    @Override
    public void initialize() throws Exception {
        this.initialized = true;

        this.queryBinder = new QueryBinder(this.sm);
        System.out.println("Query Binder initialized successfully.");
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
    public ExecutionResult executeQuery(String sqlQuery, int clientTxId) {
        MockDDLParser ddlParser = new MockDDLParser(sqlQuery);
        if (ddlParser.isDDL()) {
            return executeDDL(ddlParser);
        }

        int txId;
        boolean isAutoCommit = false;

        if (clientTxId != -1) {
            txId = clientTxId;
        } else {
            txId = ccm.beginTransaction();
            isAutoCommit = true;

            frm.writeTransactionLog(txId, "BEGIN");
        }

        ParsedQuery parsedQuery = null;
        
        try {
            // 1. Parsing & Optimization
            parsedQuery = qo.parseQuery(sqlQuery);
            if (parsedQuery == null) {
                throw new IllegalArgumentException("Query tidak valid atau tidak dikenali.");
            }

            String type = parsedQuery.queryType().toUpperCase();

            if (type.equals("BEGIN") || type.equals("BEGIN TRANSACTION")) {
                isAutoCommit = false; 
            }

            ParsedQuery boundQuery = (queryBinder != null) ? queryBinder.bind(parsedQuery) : parsedQuery;
            System.out.println(boundQuery.planRoot());
            ParsedQuery optimizedQuery = qo.optimizeQuery(boundQuery, sm.getAllStats());

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

            if (optimizedQuery.targetTables() != null) {
                List<String> tables = optimizedQuery.targetTables().stream().map(t -> "TABLE::"+t).toList();
                Response res = ccm.validateObjects(tables, txId, action);
                if (!res.isAllowed()) throw new Exception("Lock denied: " + res.reason());
            }

            // 3. Execute Plan Tree using Volcano Model
            com.apacy.common.dto.plan.PlanNode planRoot = parsedQuery.planRoot();
            String qType = parsedQuery.queryType().toUpperCase();
            boolean isModifyQuery = List.of("INSERT", "UPDATE", "DELETE").contains(qType);

            // Fallback: If Optimizer failed to set planRoot, OR if it returned a non-ModifyNode for a Modify Query
            if (planRoot == null || (isModifyQuery && !(planRoot instanceof com.apacy.common.dto.plan.ModifyNode))) {
                
                if (isModifyQuery) {
                    // Extract filter predicate if present
                    com.apacy.common.dto.plan.PlanNode childNode = null;
                    if (parsedQuery.whereClause() != null) {
                        if (!"INSERT".equals(qType)) {
                            // Create a dummy Scan + Filter structure
                            com.apacy.common.dto.plan.ScanNode dummyScan = new com.apacy.common.dto.plan.ScanNode(parsedQuery.targetTables().get(0), "t");
                            childNode = new com.apacy.common.dto.plan.FilterNode(dummyScan, parsedQuery.whereClause());
                        }
                    }

                    // Sanitize values: Extract raw literals from AST nodes if necessary
                    List<Object> rawValues = new ArrayList<>();
                    if (parsedQuery.values() != null) {
                        for (Object val : parsedQuery.values()) {
                            rawValues.add(extractValue(val));
                        }
                    }

                    planRoot = new com.apacy.common.dto.plan.ModifyNode(
                        qType, 
                        childNode, 
                        parsedQuery.targetTables().get(0), 
                        parsedQuery.targetColumns(), 
                        rawValues
                    );
                } else {
                    throw new RuntimeException("Query Optimizer returned null plan for " + qType);
                }
            }

            Operator rootOperator = planTranslator.build(planRoot, txId, sm, ccm, frm);

            List<Row> resultRows = new ArrayList<>();
            
            if (rootOperator != null) {
                rootOperator.open();
                Row row;
                while ((row = rootOperator.next()) != null) {
                    resultRows.add(row);
                }
                rootOperator.close();
            }

            boolean isTCL = type.equals("BEGIN") || type.equals("COMMIT") || type.equals("ABORT") || type.equals("ROLLBACK");
            
            if (isAutoCommit && !isTCL) {
                ccm.endTransaction(txId, true);

                frm.writeTransactionLog(txId, "COMMIT");
            }
            
            return createExecutionResult(parsedQuery, resultRows, txId);

        } catch (Exception e) {
            // Error Handling: Rollback & Recover
            ccm.endTransaction(txId, false);

            frm.writeTransactionLog(txId, "ABORT");

            frm.recover(new RecoveryCriteria("UNDO_TRANSACTION", String.valueOf(txId), null));
            e.printStackTrace();
            
            String opType = (parsedQuery != null) ? parsedQuery.queryType() : "UNKNOWN";
            return new ExecutionResult(false, e.getMessage(), txId, opType, 0, Collections.emptyList());
        }
    }

    private Object extractValue(Object val) {
        if (val instanceof ExpressionNode expr) {
            return extractValue(expr.term());
        }
        if (val instanceof TermNode term) {
            return extractValue(term.factor());
        }
        if (val instanceof LiteralFactor lit) {
            return lit.value();
        }
        return val;
    }

    public ExecutionResult executeQuery(String sqlQuery) {
        return executeQuery(sqlQuery, -1);
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

            // Use PlanTranslator to build Operator
            Operator op = planTranslator.build(ddlNode, 0, sm, ccm, frm);
            op.open();
            op.close();

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
