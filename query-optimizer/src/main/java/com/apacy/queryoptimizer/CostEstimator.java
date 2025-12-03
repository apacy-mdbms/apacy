package com.apacy.queryoptimizer;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.ExpressionNode.TermPair;
import com.apacy.common.dto.ast.expression.FactorNode;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.expression.TermNode.FactorPair;
import com.apacy.common.dto.ast.where.BinaryConditionNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.ast.where.LiteralConditionNode;
import com.apacy.common.dto.ast.where.UnaryConditionNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;
import com.apacy.common.dto.plan.CartesianNode;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.LimitNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.dto.plan.SortNode;

/**
 * Cost estimator for query execution plans.
 */
public class CostEstimator {

    private final double tS;
    private final double tT;

    private record DerivedCost (
        double cost,
        int nr,
        int br,
        int lr
    ) {}

    private record SelectivityResult (
        double sel,
        int nr
    ) {}

    public CostEstimator() {
        this(4, 0.1);
    }

    public CostEstimator(double tS, double tT) {
        this.tS = tS;
        this.tT = tT;
    }

    public double estimatePlanCost(PlanNode plan, Map<String, Statistic> stats) {
        DerivedCost derivedCost = estimatePlanCostHelper(plan, stats);
        return derivedCost.cost();
    }

    public DerivedCost estimatePlanCostHelper(PlanNode plan, Map<String, Statistic> stats) {
        if (plan instanceof ScanNode scan) {
            return costScan(scan, stats);
        } else if (plan instanceof FilterNode filter) {
            return costFilter(filter, stats);
        } else if (plan instanceof ProjectNode project) {
            return costProject(project, stats);
        } else if (plan instanceof SortNode sort) {
            return costSort(sort, stats);
        } else if (plan instanceof JoinNode join) {
            return costJoin(join, stats);
        } else if (plan instanceof CartesianNode cartesian) {
            return costCartesian(cartesian, stats);
        } else if (plan instanceof LimitNode limit) {
            return costLimit(limit, stats);
        }
        System.out.println("Unknown Node");
        for (PlanNode child : plan.getChildren()) {
            return estimatePlanCostHelper(child, stats);
        }
        throw new RuntimeException("Unknown Node has no children");
    }

    private DerivedCost costScan(ScanNode scan, Map<String,Statistic> stats) {
        Statistic s = stats.get(scan.tableName());

        // full table scan:
        int nr = s.nr();
        int br = s.br();
        int lr = s.lr();

        double cost =  (1 * tS) + (br * tT);
        return new DerivedCost(cost, nr, br, lr);
    }

    private DerivedCost costFilter(FilterNode filter, Map<String,Statistic> stats) {
        PlanNode child = filter.getChildren().get(0);

        DerivedCost childCost = estimatePlanCostHelper(child, stats);

        return childCost;
    }

    private SelectivityResult estimateSelectivityHelper(WhereConditionNode conditionNode, Map<String, Statistic> stats, DerivedCost derivedCost) {
        if (conditionNode instanceof BinaryConditionNode bin) {
            SelectivityResult left = estimateSelectivityHelper(bin.left(), stats, derivedCost);
            SelectivityResult right = estimateSelectivityHelper(bin.right(), stats, derivedCost);

            double sel = 0.0;
            switch (bin.operator().toUpperCase()) {
                case "AND":
                    // return use conjunction
                    sel = left.sel() * right.sel();
                    return new SelectivityResult(sel, (int)(derivedCost.nr() * sel));
                case "OR":
                    // return use Disjunction
                    sel = 1 - (1 - left.sel()) * (1 - right.sel());
                    return new SelectivityResult(sel, (int)(derivedCost.nr() * sel));
                default:
                    throw new RuntimeException("Illegal binary condition operator");
            }
        }
        else if (conditionNode instanceof UnaryConditionNode unary) {
            if (!"NOT".equalsIgnoreCase(unary.operator())) {
                throw new RuntimeException("Illegal unary condition operator");
            }
            SelectivityResult operand = estimateSelectivityHelper(unary.operand(), stats, derivedCost);
            double sel = 1 - operand.sel(); // negation
            return new SelectivityResult(sel, (int)(derivedCost.nr() * sel));
        }
        else if (conditionNode instanceof ComparisonConditionNode comp) {
            String leftAttr = getExpressionAttribute(comp.leftOperand());
            String rightAttr = getExpressionAttribute(comp.rightOperand());
            if (leftAttr == null && rightAttr == null) {
                return new SelectivityResult(1, derivedCost.nr());
            }
            if (leftAttr == null ^ rightAttr == null) {
                return new SelectivityResult(0,0);
            }
            String singleAttribute = leftAttr == null ? rightAttr : leftAttr;

            String tableName = singleAttribute.substring(0, singleAttribute.indexOf('.'));
            String columnName = singleAttribute.substring(singleAttribute.indexOf('.') + 1);
            double sel = 1.0;
            Map<String, Integer> Vs = stats.get(tableName).V();
            switch (comp.operator().toUpperCase()) {
                case "=":
                    // return use equality
                    if (V != null)
                        sel = 1.0 / V;
                    return new SelectivityResult(sel, (int)(nr * sel));
                case "<":
                case "<=":
                    if (min != null && max != null) {
                        int v = (int) value;
                        if (v <= min) sel = 0;
                        else if (v >= max) sel = 1;
                        else sel = (double)(v - min) / (double)(max - min);
                    }
                    return new SelectivityResult(sel, (int)(nr * sel));
                case ">":
                case ">=":
                    // return use inequality
                    if (min != null && max != null) {
                        int v = (int) value;
                        if (v >= max) sel = 0;
                        else if (v <= min) sel = 1;
                        else sel = (double)(max - v) / (double)(max - min);
                    }
                    return new SelectivityResult(sel, (int)(nr * sel));
                default:
                    throw new RuntimeException("Illegal binary condition operator");
            }
        }
        if (conditionNode instanceof LiteralConditionNode lit) {
            if (lit.value())
                return new SelectivityResult(1.0, derivedCost.nr());
            else
                return new SelectivityResult(0.0, 0);
        }
    }

    // book only allow one attribute
    private String getExpressionAttribute(ExpressionNode expr) {
        String first = getTermAttribute(expr.term());
        if (first == null) return first;
        for (TermPair remainder : expr.remainderTerms()) {
            String next = getTermAttribute(remainder.term());
            if (next != null) return null;
        }
        return first;
    }

    private String getTermAttribute(TermNode term) {
        String first = getFactorAttribute(term.factor());
        if (first == null) return first;
        for (FactorPair remainder : term.remainderFactors()) {
            String next = getFactorAttribute(remainder.factor());
            if (next != null) return null;
        }
        return first;
    }

    private String getFactorAttribute(FactorNode factor) {
        if (factor instanceof ColumnFactor col) {
            String colName = col.columnName();

            if (colName.contains(".")) {
                colName = colName.substring(colName.indexOf('.') + 1);
            }
            return colName;
        }
        return null;
    }


    private DerivedCost estimateSelectivity(WhereConditionNode conditionNode, Map<String, Statistic> stats, DerivedCost derivedCost) {
        SelectivityResult res = estimateSelectivityHelper(conditionNode, stats, derivedCost);
        return new DerivedCost(derivedCost.cost(), res.nr(), (int)(derivedCost.br() * res.sel()), derivedCost.lr());

    }


    private DerivedCost costProject(ProjectNode proj, Map<String,Statistic> stats) {
        // projection is pipelined
        return estimatePlanCostHelper(proj.getChildren().get(0), stats);
    }

    private DerivedCost costSort(SortNode sort, Map<String,Statistic> stats) {
        PlanNode child = sort.getChildren().get(0);
        DerivedCost childCost = estimatePlanCostHelper(child, stats);

        double sortCost = childCost.nr() * (Math.log(childCost.nr()) / Math.log(2));
        return new DerivedCost(childCost.cost() + sortCost, childCost.nr(), childCost.br(), childCost.lr());
    }

    private DerivedCost costLimit(LimitNode limit, Map<String,Statistic> stats) {
        return estimatePlanCostHelper(limit.getChildren().get(0), stats);
    }

    private DerivedCost costJoin(JoinNode join, Map<String,Statistic> stats) {
        PlanNode left = join.getChildren().get(0);
        PlanNode right = join.getChildren().get(1);

        DerivedCost leftCost = estimatePlanCostHelper(left, stats);
        DerivedCost rightCost = estimatePlanCostHelper(right, stats);

        int bS = leftCost.br();
        int bR = rightCost.br();

        double cost = (bR * bS + bR)*tT + 2 * bR;

        return new DerivedCost(leftCost.cost()+rightCost.cost()+cost, bS, bR, leftCost.lr());
    }

    private DerivedCost costCartesian(CartesianNode join, Map<String,Statistic> stats) {
        PlanNode left = join.getChildren().get(0);
        PlanNode right = join.getChildren().get(1);

        DerivedCost leftCost = estimatePlanCostHelper(left, stats);
        DerivedCost rightCost = estimatePlanCostHelper(right, stats);

        int bS = leftCost.br();
        int bR = rightCost.br();

        double cost = (bR * bS + bR)*tT + 2 * bR;

        return new DerivedCost(leftCost.cost()+rightCost.cost()+cost, bS, bR, leftCost.lr());
    }


}
