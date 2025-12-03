package com.apacy.common.dto.ast.where;

public sealed interface WhereConditionNode
    permits BinaryConditionNode, ComparisonConditionNode, UnaryConditionNode, LiteralConditionNode {}
