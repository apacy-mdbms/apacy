package com.apacy.common.dto.ast.where;

public record LiteralConditionNode(boolean value)
    implements WhereConditionNode {}