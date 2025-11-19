package com.apacy.queryoptimizer.ast.where;

import com.apacy.queryoptimizer.ast.expression.ExpressionNode;

/**
 * ComparisonConditionNode can be used for comparing expression with expression
 *
 * example: WHERE a = 15;
 */
public record ComparisonConditionNode(ExpressionNode leftOperand, String operator, ExpressionNode rightOperand)
    implements WhereConditionNode {}