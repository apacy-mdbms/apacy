package com.apacy.common.dto.ast.join;

public sealed interface JoinOperand permits TableNode, JoinConditionNode {}