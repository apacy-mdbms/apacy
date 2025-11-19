package com.apacy.queryoptimizer.ast.join;

public record TableNode(String tableName) implements JoinOperand {}