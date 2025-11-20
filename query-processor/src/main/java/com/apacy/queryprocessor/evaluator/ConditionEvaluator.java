package com.apacy.queryprocessor.evaluator;

import com.apacy.common.dto.Row;
import com.apacy.queryoptimizer.ast.expression.*;
import com.apacy.queryoptimizer.ast.where.*;

import java.util.Map;

public class ConditionEvaluator {

    public static boolean evaluate(Row row, Object conditionNode) {
        if (conditionNode == null) return true;

        if (conditionNode instanceof BinaryConditionNode bin) {
            boolean left = evaluate(row, bin.left());
            boolean right = evaluate(row, bin.right());
            
            switch (bin.operator().toUpperCase()) {
                case "AND": return left && right;
                case "OR": return left || right;
                default: return false;
            };
        } 
        else if (conditionNode instanceof UnaryConditionNode unary) {
            boolean operand = evaluate(row, unary.operand());
            if ("NOT".equalsIgnoreCase(unary.operator())) {
                return !operand;
            }
        } 
        else if (conditionNode instanceof ComparisonConditionNode comp) {
            Object leftVal = evaluateExpression(row, comp.leftOperand());
            Object rightVal = evaluateExpression(row, comp.rightOperand());
            return compare(leftVal, comp.operator(), rightVal);
        }
        else if (conditionNode instanceof LiteralConditionNode lit) {
            return lit.value();
        }

        return true; // Default fail-safe
    }

    private static Object evaluateExpression(Row row, ExpressionNode expr) {
        return evaluateTerm(row, expr.term());
    }

    private static Object evaluateTerm(Row row, TermNode term) {
        return evaluateFactor(row, term.factor());
    }

    private static Object evaluateFactor(Row row, FactorNode factor) {
        if (factor instanceof LiteralFactor lit) {
            return lit.value();
        } else if (factor instanceof ColumnFactor col) {
            String colName = col.columnName();
            
            // 1. Cek Exact Match
            if (row.data().containsKey(colName)) {
                return row.data().get(colName);
            }
            
            // 2. Cek Suffix Match (handle 'dosen.nidn' vs 'nidn')
            for (String key : row.data().keySet()) {
                if (key.endsWith("." + colName) || key.equals(colName)) {
                    return row.data().get(key);
                }
            }
            return null; 
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean compare(Object v1, String op, Object v2) {
        if (v1 == null || v2 == null) return false; 
        if ((v1 instanceof String && v2 instanceof Number) || (v1 instanceof Number && v2 instanceof String)) {
            try {
                // Prioritas: Coba convert ke Double untuk perbandingan angka (misal: "10101" vs 10101)
                Double d1 = Double.parseDouble(v1.toString());
                Double d2 = Double.parseDouble(v2.toString());
                int cmp = Double.compare(d1, d2);
                return checkOp(cmp, op);
            } catch (NumberFormatException e) {
                int cmp = v1.toString().compareTo(v2.toString());
                return checkOp(cmp, op);
            }
        }

        // 1. Numeric Comparison
        if (v1 instanceof Number n1 && v2 instanceof Number n2) {
            double d1 = n1.doubleValue();
            double d2 = n2.doubleValue();
            int cmp = Double.compare(d1, d2);
            return checkOp(cmp, op);
        }

        // 2. Comparable Comparison (String vs String, dll)
        if (v1 instanceof Comparable c1 && v2.getClass().isAssignableFrom(v1.getClass())) {
            int cmp = c1.compareTo(v2);
            return checkOp(cmp, op);
        }
        
        // 3. Fallback Equality
        if (op.equals("=") || op.equals("==")) return v1.toString().equals(v2.toString()); // Force string check
        if (op.equals("<>") || op.equals("!=")) return !v1.toString().equals(v2.toString());

        return false;
    }

    private static boolean checkOp(int cmp, String op) {
        switch (op) {
            case "=": return cmp == 0;
            case ">": return cmp > 0;
            case "<": return cmp < 0;
            case ">=": return cmp >= 0;
            case "<=": return cmp <= 0;
            case "<>", "!=": return cmp != 0;
            default: return false;
        };
    }
}