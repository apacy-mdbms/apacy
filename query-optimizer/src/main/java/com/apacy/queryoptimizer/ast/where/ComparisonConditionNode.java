package com.apacy.queryoptimizer.ast.where;

/**
 * ComparisonConditionNode can be used for comparing column with literal
 *
 * example: WHERE a = 15;
 */
public record ComparisonConditionNode(String leftOperand, String operator, Object rightOperand)
    implements WhereConditionNode {}