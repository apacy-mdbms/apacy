package com.apacy.queryoptimizer.ast.join;

public sealed interface JoinOperand permits TableNode, JoinConditionNode {}