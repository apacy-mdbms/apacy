package com.apacy.queryprocessor.evaluator;

import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.FactorNode;
import com.apacy.common.dto.ast.expression.LiteralFactor;
import com.apacy.common.dto.ast.expression.TermNode;

/**
 * Utility class to evaluate AST ExpressionNodes.
 */
public class ExpressionEvaluator {
    public static Object evaluate(Object node) {
        if (node == null) return null;

        if (node instanceof String || node instanceof Number || node instanceof Boolean) {
            return node;
        }

        if (node instanceof ExpressionNode expr) {
            return evaluateExpression(expr);
        }
        if (node instanceof TermNode term) {
            return evaluateTerm(term);
        }
        if (node instanceof FactorNode factor) {
            return evaluateFactor(factor);
        }

        return node;
    }

    private static Object evaluateExpression(ExpressionNode expr) {
        if (expr == null) return null;
        return evaluateTerm(expr.term());
    }

    private static Object evaluateTerm(TermNode term) {
        if (term == null) return null;
        return evaluateFactor(term.factor());
    }

    private static Object evaluateFactor(FactorNode factor) {
        if (factor == null) return null;

        if (factor instanceof LiteralFactor lit) {
            return lit.value();
        }
        if (factor instanceof ExpressionNode nestedExpr) {
            return evaluateExpression(nestedExpr);
        }
        if (factor instanceof ColumnFactor col) {
            throw new UnsupportedOperationException("Column reference '" + col.columnName() + "' not supported in this value context.");
        }

        return null;
    }
}