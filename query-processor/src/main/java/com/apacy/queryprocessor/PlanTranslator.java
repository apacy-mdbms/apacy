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
import com.apacy.queryprocessor.execution.SortMergeJoinOperator;
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
        
        // Extract join column for optimization (Simple Equi-Join)
        String joinColumn = extractJoinColumn(node.joinCondition());
        
        if (joinColumn != null) {
            // Choose join strategy based on heuristics
            JoinStrategy strategy = selectJoinStrategy(node, joinColumn);
            
            switch (strategy) {
                case SORT_MERGE:
                    System.out.println("[PlanTranslator] Selected SortMergeJoin strategy for column: " + joinColumn);
                    return new SortMergeJoinOperator(left, right, joinColumn);
                    
                case HASH:
                    System.out.println("[PlanTranslator] Selected HashJoin strategy for column: " + joinColumn);
                    return new HashJoinOperator(left, right, Collections.singletonList(joinColumn));
                    
                case NESTED_LOOP:
                default:
                    System.out.println("[PlanTranslator] Selected NestedLoopJoin strategy (fallback)");
                    return new NestedLoopJoinOperator(left, right, node.joinCondition());
            }
        } else {
            // Complex join condition - fallback to Nested Loop Join
            System.out.println("[PlanTranslator] Complex join condition detected, using NestedLoopJoin");
            return new NestedLoopJoinOperator(left, right, node.joinCondition());
        }
    }
    
    /**
     * Enum untuk strategi join yang tersedia
     */
    private enum JoinStrategy {
        HASH,           // Hash Join - efisien untuk ukuran sedang
        SORT_MERGE,     // Sort-Merge Join - efisien jika data sudah terurut
        NESTED_LOOP     // Nested Loop Join - fallback untuk kasus umum
    }
    
    /**
     * Memilih strategi join terbaik berdasarkan karakteristik input.
     * 
     * Heuristik:
     * 1. Jika salah satu child adalah SortNode pada join column yang sama -> SortMergeJoin
     *    (data sudah terurut, manfaatkan untuk efisiensi)
     * 2. Jika estimasi ukuran data kecil-menengah -> HashJoin
     *    (efisien untuk ukuran sedang, memory overhead acceptable)
     * 3. Untuk kasus lain -> NestedLoopJoin
     *    (safe fallback, tidak butuh preprocessing)
     */
    private Operator buildJoin(JoinNode node, int txId, IStorageManager sm, IConcurrencyControlManager ccm, IFailureRecoveryManager frm) {
        Operator left = build(node.left(), txId, sm, ccm, frm);
        Operator right = build(node.right(), txId, sm, ccm, frm);
        
        JoinAlgorithm algo = (node.algorithm() != null) ? node.algorithm() : JoinAlgorithm.NESTED_LOOP;

        // Ekstrak kolom join (jika ada equality join) untuk Hash/SortMerge
        String joinColumn = extractJoinColumn(node.joinCondition());

        // Dispatch
        switch (algo) {
            case SORT_MERGE:
                if (joinColumn == null) {
                    System.err.println("[PlanTranslator] Warning: SortMerge requested but no equality condition found. Falling back to NestedLoop.");
                    return new NestedLoopJoinOperator(left, right, node.joinCondition());
                }
                return new SortMergeJoinOperator(left, right, joinColumn);

            case HASH:
                if (joinColumn == null) {
                    System.err.println("[PlanTranslator] Warning: HashJoin requested but no equality condition found. Falling back to NestedLoop.");
                    return new NestedLoopJoinOperator(left, right, node.joinCondition());
                }
                return new HashJoinOperator(left, right, Collections.singletonList(joinColumn));

            case CARTESIAN:
                return new CartesianOperator(left, right);

            case NESTED_LOOP:
            default:
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
    
    /**
     * Check if a PlanNode produces output sorted by the given column.
     * Currently checks if the node is directly a SortNode on that column.
     */
    private boolean isNodeSortedBy(PlanNode node, String column) {
        if (node instanceof SortNode sortNode) {
            return sortNode.sortColumn().equals(column);
        }
        // Could be extended to check for indexed scans, etc.
        return false;
    }
    
    /**
     * Estimate the number of rows that will be produced by a PlanNode.
     * This is a simple heuristic - in a real system, this would use statistics.
     */
    private int estimateNodeSize(PlanNode node) {
        if (node instanceof ScanNode) {
            // Rough estimate: assume tables are medium-sized
            return 10000;
        }
        if (node instanceof FilterNode filterNode) {
            // Assume filter reduces size by ~50%
            return estimateNodeSize(filterNode.child()) / 2;
        }
        if (node instanceof ProjectNode projectNode) {
            // Projection doesn't change row count
            return estimateNodeSize(projectNode.child());
        }
        if (node instanceof SortNode sortNode) {
            // Sort doesn't change row count
            return estimateNodeSize(sortNode.child());
        }
        if (node instanceof LimitNode limitNode) {
            // Limit reduces to specified count
            return limitNode.limit();
        }
        // Default: unknown size, be conservative
        return -1; // Unknown
    }
}
