package com.apacy.queryoptimizer.ast.expression;


public sealed interface FactorNode
    permits ExpressionNode, LiteralFactor, ColumnFactor {}