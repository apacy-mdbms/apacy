package com.apacy.common.dto.ast.expression;


public sealed interface FactorNode
    permits ExpressionNode, LiteralFactor, ColumnFactor {}