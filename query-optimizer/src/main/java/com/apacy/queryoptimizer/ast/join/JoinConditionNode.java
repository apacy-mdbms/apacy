package com.apacy.queryoptimizer.ast.join;

import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

/**
 * AST for join clauses.
 * Uses WhereConditionNode for conditions on join.
 *
 * example: A JOIN B ON A.id = B.id JOIN c ON B.id2 = C.id2;
 *
 *               JoinConditionNode
 *               /               \
 *       JoinConditionNode   TableNode(C)
 *        /             \
 *   TableNode(A)  TableNode(B)
 *
 *  Join node at root has conditions       : ComparisonConditionNode(B.id2, =, C.id2)
 *  Join node at lower lever has conditions: ComparisonConditionNode(A.id , =, C.Id)
 *
 */
public record JoinConditionNode(
    String joinType, // INNER, CROSS
    JoinOperand left,
    JoinOperand right,
    WhereConditionNode conditions
) implements JoinOperand {}