package com.apacy.queryoptimizer;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.ExpressionNode.TermPair;
import com.apacy.common.dto.ast.expression.FactorNode;
import com.apacy.common.dto.ast.expression.LiteralFactor;
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
        DerivedCost filteredCost = estimateSelectivity((WhereConditionNode)filter.predicate(), stats, childCost);

        return filteredCost;
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
            String attribute;
            Object value = null;
            if (leftAttr == null) {
                attribute = rightAttr;
                value = getExpressionValue(comp.leftOperand());
            } else {
                attribute = leftAttr;
                value = getExpressionValue(comp.rightOperand());
            }
            String singleAttribute = leftAttr == null ? rightAttr : leftAttr;

            String tableName = singleAttribute.substring(0, singleAttribute.indexOf('.'));
            String columnName = singleAttribute.substring(singleAttribute.indexOf('.') + 1);
            double sel = 1.0;
            Map<String, Integer> Vs = stats.get(tableName).V();
            double V = stats.get(tableName).V().get(columnName);
            Double max = (Double) stats.get(tableName).maxVal().get(columnName);
            Double min = (Double) stats.get(tableName).minVal().get(columnName);
            switch (comp.operator().toUpperCase()) {
                case "=":
                    // return use equality
                    if (V != 0.0)
                        sel = 1.0 / V;
                    return new SelectivityResult(sel, (int)(derivedCost.nr() * sel));
                case "<":
                case "<=":
                    if (value instanceof Number num && min != null && max != null) {
                        int v = (int) num;
                        if (v <= min) sel = 0;
                        else if (v >= max) sel = 1;
                        else sel = (double)(v - min) / (double)(max - min);
                    }
                    return new SelectivityResult(sel, (int)(derivedCost.nr() * sel));
                case ">":
                case ">=":
                    // return use inequality
                    if (value instanceof Number num && min != null && max != null) {
                        int v = (int) num;
                        if (v >= max) sel = 0;
                        else if (v <= min) sel = 1;
                        else sel = (double)(max - v) / (double)(max - min);
                    }
                    return new SelectivityResult(sel, (int)(derivedCost.nr() * sel));
                default:
                    throw new RuntimeException("Illegal binary condition operator");
            }
        } else if (conditionNode instanceof LiteralConditionNode lit) {
            if (lit.value())
                return new SelectivityResult(1.0, derivedCost.nr());
            else
                return new SelectivityResult(0.0, 0);
        }

        throw new RuntimeException("Unknown condition node");
    }

    private Object getExpressionValue(ExpressionNode expr) {
        Object first = getTermValue(expr.term());
        if (first == null) return first;
        for (TermPair remainder : expr.remainderTerms()) {
            Object next = getTermValue(remainder.term());
            if (next != null) return null;
        }
        return first;
    }

    private Object getTermValue(TermNode term) {
        Object first = getFactorValue(term.factor());
        if (first == null) return first;
        for (FactorPair remainder : term.remainderFactors()) {
            Object next = getFactorValue(remainder.factor());
            if (next != null) return null;
        }
        return first;
    }

    private Object getFactorValue(FactorNode factor) {
        if (factor instanceof LiteralFactor literal) {
            return literal.value();
        } else if (factor instanceof ExpressionNode expr) {
            return getExpressionValue(expr);
        }
        return null;
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
        } else if (factor instanceof ExpressionNode expr) {
            return getExpressionAttribute(expr);
        }
        return null;
    }


    private DerivedCost estimateSelectivity(WhereConditionNode conditionNode, Map<String, Statistic> stats, DerivedCost derivedCost) {
        SelectivityResult res = estimateSelectivityHelper(conditionNode, stats, derivedCost);
        return new DerivedCost(derivedCost.cost(), res.nr(), (int)(derivedCost.br() * res.sel()), derivedCost.lr());

    }


    private DerivedCost costProject(ProjectNode proj, Map<String,Statistic> stats) {
        DerivedCost childCost =  estimatePlanCostHelper(proj.getChildren().get(0), stats);
        // asumsi lr menjadi 2x lebih kecil
        int blockSize = 4096;
        int lr2 = childCost.lr() / 2;
        int fr2 = (int)Math.floor(blockSize / lr2);
        int br2 = (int)Math.ceil(childCost.nr() / fr2);
        return new DerivedCost(childCost.cost(), childCost.nr(), br2, lr2);
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
