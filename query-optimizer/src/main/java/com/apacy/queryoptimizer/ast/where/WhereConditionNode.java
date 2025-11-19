package com.apacy.queryoptimizer.ast.where;

public sealed interface WhereConditionNode
    permits BinaryConditionNode, ComparisonConditionNode, UnaryConditionNode, LiteralConditionNode {}
