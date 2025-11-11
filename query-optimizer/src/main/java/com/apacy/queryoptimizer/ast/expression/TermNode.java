package com.apacy.queryoptimizer.ast.expression;

import java.util.List;

public record TermNode(FactorNode factor, List<FactorPair> remainderFactors) {
    public record FactorPair(char multiplicativeOperator, FactorNode factor) {}
}
