package com.apacy.queryoptimizer.ast.expression;

import java.util.List;

public record ExpressionNode(TermNode term, List<TermPair> remainderTerms) implements FactorNode {
    public record TermPair(char additiveOperator, TermNode term) {}
}