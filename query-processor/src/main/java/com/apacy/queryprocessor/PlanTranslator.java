package com.apacy.queryprocessor;

import java.util.Collections;

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
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.queryprocessor.execution.CartesianOperator;
import com.apacy.queryprocessor.execution.DDLOperator;
import com.apacy.queryprocessor.execution.FilterOperator;
import com.apacy.queryprocessor.execution.HashJoinOperator;
import com.apacy.queryprocessor.execution.LimitOperator;
import com.apacy.queryprocessor.execution.ModifyOperator;
import com.apacy.queryprocessor.execution.NestedLoopJoinOperator;
import com.apacy.queryprocessor.execution.Operator;
import com.apacy.queryprocessor.execution.ProjectOperator;
import com.apacy.queryprocessor.execution.ScanOperator;
import com.apacy.queryprocessor.execution.SortOperator;
import com.apacy.queryprocessor.execution.TCLOperator;

/**
 * Builds the Execution Pipeline (Operator Tree) from the Query Plan.
 * Replaces the old direct-execution model.
 */
public class PlanTranslator {

    public Operator build(PlanNode node, int txId, IStorageManager sm, IConcurrencyControlManager ccm, IFailureRecoveryManager frm) {
        if (node == null) {
            return null;
        }

        if (node instanceof ScanNode n) {
            return new ScanOperator(n, sm);
        }
        if (node instanceof FilterNode n) {
            return new FilterOperator(build(n.child(), txId, sm, ccm, frm), n.predicate());
        }
        if (node instanceof ProjectNode n) {
            return new ProjectOperator(build(n.child(), txId, sm, ccm, frm), n.columns());
        }
        if (node instanceof JoinNode n) {
            return buildJoin(n, txId, sm, ccm, frm);
        }
        if (node instanceof SortNode n) {
            return new SortOperator(build(n.child(), txId, sm, ccm, frm), n);
        }
        if (node instanceof LimitNode n) {
            return new LimitOperator(build(n.child(), txId, sm, ccm, frm), n);
        }
        if (node instanceof ModifyNode n) {
            Operator childOp = null;
            if (!n.getChildren().isEmpty()) {
                childOp = build(n.getChildren().get(0), txId, sm, ccm, frm);
            }

            return new ModifyOperator(
                    n,
                    childOp,
                    sm,
                    ccm,
                    frm,
                    txId
            );
        }
        if (node instanceof DDLNode n) {
            return new DDLOperator(n, sm);
        }
        if (node instanceof TCLNode n) {
            return new TCLOperator(n, ccm, txId, frm);
        }
        if (node instanceof CartesianNode n) {
            return new CartesianOperator(build(n.left(), txId, sm, ccm, frm), build(n.right(), txId, sm, ccm, frm));
        }

        throw new UnsupportedOperationException("PlanNode type not supported: " + node.getClass().getSimpleName());
    }

    private Operator buildJoin(JoinNode node, int txId, IStorageManager sm, IConcurrencyControlManager ccm, IFailureRecoveryManager frm) {
        Operator left = build(node.left(), txId, sm, ccm, frm);
        Operator right = build(node.right(), txId, sm, ccm, frm);
        
        // Extract join column for Hash Join optimization (Simple Equi-Join)
        String joinColumn = extractJoinColumn(node.joinCondition());
        
        if (joinColumn != null) {
            return new HashJoinOperator(left, right, Collections.singletonList(joinColumn));
        } else {
            // Fallback to Nested Loop Join
            return new NestedLoopJoinOperator(left, right, node.joinCondition());
        }
    }
    
    // ==================================================================================
    // HELPER METHODS (Preserved from original implementation)
    // ==================================================================================

    private String extractJoinColumn(Object condition) {
        if (!(condition instanceof com.apacy.common.dto.ast.where.ComparisonConditionNode)) {
            return null;
        }
        
        com.apacy.common.dto.ast.where.ComparisonConditionNode comp = 
            (com.apacy.common.dto.ast.where.ComparisonConditionNode) condition;
        
        // Only support "="
        if (!"=".equals(comp.operator())) {
            return null;
        }
        
        String leftCol = extractColumnNameFromExpression(comp.leftOperand());
        String rightCol = extractColumnNameFromExpression(comp.rightOperand());
        
        if (leftCol != null && rightCol != null && leftCol.equals(rightCol)) {
            return leftCol;
        }
        
        return null;
    }
    
    /**
     * Ekstrak nama kolom dari ExpressionNode
     * Method ini mirip dengan extractColumnsFromExpression di DistributeProjectRewriter
     */
    private String extractColumnNameFromExpression(com.apacy.common.dto.ast.expression.ExpressionNode expr) {
        if (expr == null || expr.term() == null) {
            return null;
        }
        
        String colName = extractColumnNameFromTerm(expr.term());
        if (colName != null) {
            return colName;
        }
        
        if (expr.remainderTerms() != null && !expr.remainderTerms().isEmpty()) {
            for (var pair : expr.remainderTerms()) {
                colName = extractColumnNameFromTerm(pair.term());
                if (colName != null) {
                    return colName;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Ekstrak nama kolom dari TermNode
     */
    private String extractColumnNameFromTerm(com.apacy.common.dto.ast.expression.TermNode term) {
        if (term == null || term.factor() == null) {
            return null;
        }
        
        // Cek factor utama
        if (term.factor() instanceof com.apacy.common.dto.ast.expression.ColumnFactor colFactor) {
            String colName = colFactor.columnName();
            if (colName.contains(".")) {
                String[] parts = colName.split("[.]");
                return parts[parts.length - 1];
            }
            return colName;
        }
        
        if (term.remainderFactors() != null && !term.remainderFactors().isEmpty()) {
            for (var pair : term.remainderFactors()) {
                if (pair.factor() instanceof com.apacy.common.dto.ast.expression.ColumnFactor colFactor) {
                    String colName = colFactor.columnName();
                    if (colName.contains(".")) {
                        String[] parts = colName.split("[.]");
                        return parts[parts.length - 1];
                    }
                    return colName;
                }
            }
        }
        return null;
    }
}
