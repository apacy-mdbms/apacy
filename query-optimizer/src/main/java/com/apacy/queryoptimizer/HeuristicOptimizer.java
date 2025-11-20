package com.apacy.queryoptimizer;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Statistic;
import com.apacy.queryoptimizer.ast.expression.ColumnFactor;
import com.apacy.queryoptimizer.ast.expression.ExpressionNode;
import com.apacy.queryoptimizer.ast.expression.TermNode;
import com.apacy.queryoptimizer.ast.expression.FactorNode;
import com.apacy.queryoptimizer.ast.join.JoinConditionNode;
import com.apacy.queryoptimizer.ast.join.JoinOperand;
import com.apacy.queryoptimizer.ast.join.TableNode;
import com.apacy.queryoptimizer.ast.where.ComparisonConditionNode;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Heuristic-based query optimizer that applies optimization rules.
 *
 * Implemented: equivalence rule (Filter + Cartesian -> Theta Join) for the simple
 * case where the WHERE clause is a single comparison between columns of the two
 * tables in a CROSS join. More complete predicate-pushing and predicate extraction
 * is TODO.
 */
public class HeuristicOptimizer {

    /**
     * Optimize a parsed query using heuristic rules.
     */
    public ParsedQuery optimize(ParsedQuery query, Map<String, Statistic> allStats) {
        // For now, optimization is rewrite-based only
        return rewrite(query);
    }

    /**
     * Generate the best execution plan from multiple alternatives.
     * TODO: Implement plan generation with cost comparison
     */
    public ParsedQuery generateBestPlan(ParsedQuery query, Map<String, Statistic> allStats) {
        // TODO: Generate and compare multiple execution plans
        throw new UnsupportedOperationException("generateBestPlan not implemented yet");
    }

    /**
     * Rewrite query using transformation rules.
     * Currently implements: if join is a CROSS (cartesian) and WHERE is a single
     * comparison between columns from the left and right table, convert to INNER join
     * with that comparison as join condition (theta-join) and remove the predicate from WHERE.
     */
    public ParsedQuery rewrite(ParsedQuery query) {
        Object joinObj = query.joinConditions();
        Object whereObj = query.whereClause();

        if (joinObj instanceof JoinConditionNode joinNode && whereObj instanceof ComparisonConditionNode comp) {
            String joinType = joinNode.joinType();
            if (joinType != null && "CROSS".equalsIgnoreCase(joinType)) {
                String leftTable = findLeftmostTable(joinNode.left());
                String rightTable = findRightmostTable(joinNode.right());

                if (leftTable != null && rightTable != null) {
                    Set<String> leftRefs = collectTablesFromExpression(comp.leftOperand());
                    Set<String> rightRefs = collectTablesFromExpression(comp.rightOperand());

                    boolean lhsLeftRhsRight = leftRefs.contains(leftTable) && rightRefs.contains(rightTable);
                    boolean lhsRightRhsLeft = leftRefs.contains(rightTable) && rightRefs.contains(leftTable);

                    if (lhsLeftRhsRight || lhsRightRhsLeft) {
                        JoinConditionNode newJoin = new JoinConditionNode("INNER", joinNode.left(), joinNode.right(), comp);

                        return new ParsedQuery(
                            query.queryType(),
                            query.targetTables(),
                            query.targetColumns(),
                            query.values(),
                            newJoin,
                            null,
                            query.orderByColumn(),
                            query.isDescending(),
                            true 
                        );
                    }
                }
            }
        }
        return query;
    }

    private static String findLeftmostTable(JoinOperand op) {
        if (op instanceof TableNode t) return t.tableName();
        if (op instanceof JoinConditionNode j) return findLeftmostTable(j.left());
        return null;
    }

    private static String findRightmostTable(JoinOperand op) {
        if (op instanceof TableNode t) return t.tableName();
        if (op instanceof JoinConditionNode j) return findRightmostTable(j.right());
        return null;
    }

    private static Set<String> collectTablesFromExpression(ExpressionNode expr) {
        Set<String> tables = new HashSet<>();
        if (expr == null) return tables;

    // check main term
    collectTablesFromTerm(expr.term(), tables);
        // check remainder terms
        if (expr.remainderTerms() != null) {
            for (ExpressionNode.TermPair pair : expr.remainderTerms()) {
                collectTablesFromTerm(pair.term(), tables);
            }
        }
        return tables;
    }

    private static void collectTablesFromTerm(TermNode term, Set<String> tables) {
        if (term == null) return;
        collectTablesFromFactor(term.factor(), tables);
        if (term.remainderFactors() != null) {
            for (com.apacy.queryoptimizer.ast.expression.TermNode.FactorPair fp : term.remainderFactors()) {
                collectTablesFromFactor(fp.factor(), tables);
            }
        }
    }

    private static void collectTablesFromFactor(FactorNode factor, Set<String> tables) {
        if (factor == null) return;
        if (factor instanceof ColumnFactor cf) {
            String col = cf.columnName();
            if (col != null && col.contains(".")) {
                String table = col.split("\\.", 2)[0];
                tables.add(table);
            }
        } else if (factor instanceof ExpressionNode en) {
            tables.addAll(collectTablesFromExpression(en));
        }
    }
}