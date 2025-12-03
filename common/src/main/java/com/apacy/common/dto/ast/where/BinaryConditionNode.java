package com.apacy.common.dto.ast.where;

/**
 * BinaryConditionNode used for combining two condition nodes by an operator.
 *
 * example: WHERE NOT false AND a = 15;
 *
 *            Binary(AND)
 *         /             \
 *      Unary(NOT)     Comparison(a, =, 15)
 *        |
 *     Literal(false)
 */
public record BinaryConditionNode(WhereConditionNode left, String operator, WhereConditionNode right)
    implements WhereConditionNode {}