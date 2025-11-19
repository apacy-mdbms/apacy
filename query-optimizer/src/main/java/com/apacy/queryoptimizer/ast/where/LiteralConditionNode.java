package com.apacy.queryoptimizer.ast.where;

public record LiteralConditionNode(boolean value)
    implements WhereConditionNode {}