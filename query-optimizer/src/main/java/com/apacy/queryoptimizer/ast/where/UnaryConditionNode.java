package com.apacy.queryoptimizer.ast.where;

/**
 * UnaryConditionNode used for applying unary operator like 'NOT' to a condition
 */
public record UnaryConditionNode(String operator, WhereConditionNode operand)
    implements WhereConditionNode {}