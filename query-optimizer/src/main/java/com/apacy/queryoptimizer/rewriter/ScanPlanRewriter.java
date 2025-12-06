package com.apacy.queryoptimizer.rewriter;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.ast.where.*;
import com.apacy.common.dto.ast.expression.*;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.queryoptimizer.CostEstimator;

public class ScanPlanRewriter extends PlanRewriter {

    public ScanPlanRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

   @Override
    protected PlanNode visitFilter(FilterNode node, Map<String, Statistic> allStats) {
        PlanNode child = node.child();
        if (child instanceof ScanNode scan) {
            ScanNode newScan = scan;

            List<String> a = extractColumns((WhereConditionNode) node.predicate(), scan);
            if (a != null) {
                newScan = new ScanNode(scan.tableName(), scan.alias(), a.get(1), node.predicate());
            }
            return new FilterNode(newScan, node.predicate());
        }
        return node;

    }

    private List<String> extractColumns(WhereConditionNode node, ScanNode scanNode) {
        if (node instanceof BinaryConditionNode n) {
            // extractColumns(n.left(), scanNode);
            // extractColumns(n.right(), scanNode);
            return null;
        } else if (node instanceof UnaryConditionNode n) {
            return null;
        } else if (node instanceof ComparisonConditionNode n) {
            String col1 = extractColumnsFromExpression(n.leftOperand(), scanNode);
            String col2 = extractColumnsFromExpression(n.rightOperand(), scanNode);
            // System.out.println(col1);
            // System.out.println(col2);

            if(col1 != null && col2 != null) return null;
            if(col1 != null) {
                return List.of(n.operator(), col1);
            }
            if(col2 != null) {
                return List.of(n.operator(), col2);
            }
            return null;
        }
        return null;
    }

    private String extractColumnsFromExpression(ExpressionNode expr, ScanNode scanNode) {
        if (expr == null) return null;
        // Cek TermNode di dalam Expression
        String col = extractColumnsFromTerm(expr.term(), scanNode);
        // Cek remainder terms
        if (expr.remainderTerms() != null && !expr.remainderTerms().isEmpty()) {
            return null;
        }
        return col;
    }

    private String extractColumnsFromTerm(TermNode term, ScanNode scanNode) {
        if (term == null) return null;
        // Cek FactorNode

        String col = null;
        if (term.factor() instanceof ColumnFactor c) {
            int idx = c.columnName().indexOf('.');
            if (idx == -1) {
                return null;
            }
            String tableName = c.columnName().substring(0, idx);
            String columnName = c.columnName().substring(idx);

            if (scanNode.tableName().equalsIgnoreCase(tableName) || scanNode.alias().equalsIgnoreCase(tableName)) {
                col = c.columnName();
            }
        }

        // Cek remainder factors
        if (term.remainderFactors() != null && !term.remainderFactors().isEmpty()) {
            for (var pair : term.remainderFactors()) {
                if (pair.factor() instanceof ColumnFactor c) {
                    int idx = c.columnName().indexOf('.');
                    if (idx == -1) {
                        return null;
                    }
                    String tableName = c.columnName().substring(0, idx);
                    String columnName = c.columnName().substring(idx);

                    if (scanNode.tableName().equalsIgnoreCase(tableName) || scanNode.alias().equalsIgnoreCase(tableName)) {
                        return null;
                    }
                }
            }
        }
        return col;
    }

}
