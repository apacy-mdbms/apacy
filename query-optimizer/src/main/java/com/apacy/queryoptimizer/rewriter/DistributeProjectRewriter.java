package com.apacy.queryoptimizer.rewriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.where.BinaryConditionNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.ast.where.UnaryConditionNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.queryoptimizer.CostEstimator;

/**
 * Rule 8: Distribute Project over Join
 * Mendorong proyeksi ke bawah join untuk mengurangi ukuran tuple intermediate.
 * Pola: Project(Join(A, B)) -> Project(Join(Project(A), Project(B)))
 */
public class DistributeProjectRewriter extends PlanRewriter {

    public DistributeProjectRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

    @Override
    protected PlanNode visitProject(ProjectNode node, Map<String, Statistic> allStats) {
        // 1. Rewrite child terlebih dahulu
        PlanNode child = rewrite(node.child(), allStats);
        
        // 2. Cek Pola: Apakah child adalah JoinNode?
        if (child instanceof JoinNode joinNode) {
            
            // List kolom yang diminta oleh Project di atas (L1 U L2)
            List<String> projectColumns = node.columns();
            
            // List kolom yang dibutuhkan oleh kondisi Join (Theta condition)
            Set<String> joinConditionColumns = new HashSet<>();
            if (joinNode.joinCondition() instanceof WhereConditionNode where) {
                extractColumns(where, joinConditionColumns);
            }

            // Gabungkan semua kolom yang "dibutuhkan"
            Set<String> allNeededColumns = new HashSet<>(projectColumns);
            allNeededColumns.addAll(joinConditionColumns);

            // 3. Buat Project baru untuk Left Child
            // (Simplifikasi: Kita dorong SEMUA kolom yang dibutuhkan ke KEDUA sisi
            //  karena kita belum punya Metadata Manager canggih untuk cek 'col1' itu punya tabel A atau B)
            List<String> pushedColumns = new ArrayList<>(allNeededColumns);
            
            // Jangan push down "*" karena itu berarti semua kolom, tidak menghemat apa-apa
            if (pushedColumns.contains("*")) {
                return new ProjectNode(joinNode, projectColumns);
            }

            PlanNode newLeft = new ProjectNode(joinNode.left(), pushedColumns);
            PlanNode newRight = new ProjectNode(joinNode.right(), pushedColumns);

            // 4. Buat Join baru dengan children yang sudah diproyeksi
            JoinNode newJoin = new JoinNode(newLeft, newRight, joinNode.joinCondition(), joinNode.joinType());

            // 5. Kembalikan Project asli di atas Join baru (untuk memastikan urutan/filter akhir benar)
            return new ProjectNode(newJoin, projectColumns);
        }

        if (child == node.child()) return node;
        return new ProjectNode(child, node.columns());
    }

    /**
     * Helper untuk mengambil nama kolom dari AST WhereConditionNode
     */
    private void extractColumns(WhereConditionNode node, Set<String> columns) {
        if (node instanceof BinaryConditionNode n) {
            extractColumns(n.left(), columns);
            extractColumns(n.right(), columns);
        } else if (node instanceof UnaryConditionNode n) {
            extractColumns(n.operand(), columns);
        } else if (node instanceof ComparisonConditionNode n) {
            extractColumnsFromExpression(n.leftOperand(), columns);
            extractColumnsFromExpression(n.rightOperand(), columns);
        }
    }

    private void extractColumnsFromExpression(ExpressionNode expr, Set<String> columns) {
        if (expr == null) return;
        // Cek TermNode di dalam Expression
        extractColumnsFromTerm(expr.term(), columns);
        // Cek remainder terms
        if (expr.remainderTerms() != null) {
            for (var pair : expr.remainderTerms()) {
                extractColumnsFromTerm(pair.term(), columns);
            }
        }
    }

    private void extractColumnsFromTerm(TermNode term, Set<String> columns) {
        if (term == null) return;
        // Cek FactorNode
        if (term.factor() instanceof ColumnFactor c) {
            columns.add(c.columnName());
        } else if (term.factor() instanceof ExpressionNode e) {
            extractColumnsFromExpression(e, columns);
        }
        
        // Cek remainder factors
        if (term.remainderFactors() != null) {
            for (var pair : term.remainderFactors()) {
                if (pair.factor() instanceof ColumnFactor c) {
                    columns.add(c.columnName());
                } else if (pair.factor() instanceof ExpressionNode e) {
                    extractColumnsFromExpression(e, columns);
                }
            }
        }
    }
}