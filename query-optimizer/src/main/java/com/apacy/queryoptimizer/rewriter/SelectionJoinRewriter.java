package com.apacy.queryoptimizer.rewriter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.CartesianNode;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.queryoptimizer.CostEstimator;
import com.apacy.queryoptimizer.ast.expression.ColumnFactor;
import com.apacy.queryoptimizer.ast.expression.ExpressionNode;
import com.apacy.queryoptimizer.ast.expression.TermNode;
import com.apacy.queryoptimizer.ast.where.BinaryConditionNode;
import com.apacy.queryoptimizer.ast.where.ComparisonConditionNode;
import com.apacy.queryoptimizer.ast.where.UnaryConditionNode;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

public class SelectionJoinRewriter extends PlanRewriter {

    public SelectionJoinRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

    @Override
    protected PlanNode visitFilter(FilterNode node, Map<String, Statistic> allStats) {
        PlanNode child = node.child();
        PlanNode rewrittenChild = rewrite(child, allStats);

        Object predicate = node.predicate();

        if (!(predicate instanceof WhereConditionNode where)) {
            if (child == rewrittenChild) return node;
            return new FilterNode(rewrittenChild, predicate);
        }

        if (rewrittenChild instanceof CartesianNode cart) {
            PlanNode left = cart.left();
            PlanNode right = cart.right();

            Set<String> leftAliases = new HashSet<>();
            Set<String> rightAliases = new HashSet<>();
            collectAliases(left, leftAliases);
            collectAliases(right, rightAliases);

            Set<String> referenced = new HashSet<>();
            extractColumns(where, referenced);

            Set<String> qualifiers = new HashSet<>();
            for (String col : referenced) {
                if (col == null) continue;
                int dot = col.indexOf('.');
                if (dot > 0) {
                    qualifiers.add(col.substring(0, dot));
                }
            }

            boolean usesLeft = qualifiers.stream().anyMatch(leftAliases::contains);
            boolean usesRight = qualifiers.stream().anyMatch(rightAliases::contains);

            if (usesLeft && usesRight) {
                return new JoinNode(left, right, where, "INNER");
            }

            if (usesLeft && !usesRight) {
                PlanNode newLeft = new FilterNode(left, where);
                return new CartesianNode(newLeft, right);
            }

            if (usesRight && !usesLeft) {
                PlanNode newRight = new FilterNode(right, where);
                return new CartesianNode(left, newRight);
            }

            if (child == rewrittenChild) return node;
            return new FilterNode(rewrittenChild, where);
        }

        if (rewrittenChild instanceof JoinNode joinNode) {
            PlanNode left = joinNode.left();
            PlanNode right = joinNode.right();

            Set<String> leftAliases = new HashSet<>();
            Set<String> rightAliases = new HashSet<>();
            collectAliases(left, leftAliases);
            collectAliases(right, rightAliases);

            Set<String> referenced = new HashSet<>();
            extractColumns(where, referenced);

            Set<String> qualifiers = new HashSet<>();
            for (String col : referenced) {
                if (col == null) continue;
                int dot = col.indexOf('.');
                if (dot > 0) {
                    qualifiers.add(col.substring(0, dot));
                }
            }

            boolean usesLeft = qualifiers.stream().anyMatch(leftAliases::contains);
            boolean usesRight = qualifiers.stream().anyMatch(rightAliases::contains);

            if (usesLeft && usesRight) {
                Object oldCond = joinNode.joinCondition();
                WhereConditionNode newCond;
                if (oldCond instanceof WhereConditionNode oldWhere) {
                    newCond = new BinaryConditionNode(oldWhere, "AND", where);
                } else {
                    newCond = where;
                }
                return new JoinNode(left, right, newCond, joinNode.joinType());
            }

            if (usesLeft && !usesRight) {
                PlanNode newLeft = new FilterNode(left, where);
                return new JoinNode(newLeft, right, joinNode.joinCondition(), joinNode.joinType());
            }

            if (usesRight && !usesLeft) {
                PlanNode newRight = new FilterNode(right, where);
                return new JoinNode(left, newRight, joinNode.joinCondition(), joinNode.joinType());
            }

            if (child == rewrittenChild) return node;
            return new FilterNode(rewrittenChild, where);
        }

        if (child == rewrittenChild) return node;
        return new FilterNode(rewrittenChild, where);
    }

    private void collectAliases(PlanNode node, Set<String> aliases) {
        if (node instanceof ScanNode s) {
            if (s.alias() != null && !s.alias().isBlank()) aliases.add(s.alias());
            if (s.tableName() != null && !s.tableName().isBlank()) aliases.add(s.tableName());
            return;
        }
        for (var child : node.getChildren()) {
            collectAliases(child, aliases);
        }
    }

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
        extractColumnsFromTerm(expr.term(), columns);
        if (expr.remainderTerms() != null) {
            for (var pair : expr.remainderTerms()) {
                extractColumnsFromTerm(pair.term(), columns);
            }
        }
    }

    private void extractColumnsFromTerm(TermNode term, Set<String> columns) {
        if (term == null) return;
        if (term.factor() instanceof ColumnFactor c) {
            columns.add(c.columnName());
        } else if (term.factor() instanceof ExpressionNode e) {
            extractColumnsFromExpression(e, columns);
        }

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
