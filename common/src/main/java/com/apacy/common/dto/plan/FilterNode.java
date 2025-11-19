package com.apacy.common.dto.plan;

import java.util.List;

// Menggunakan Object condition (bisa diganti AST WhereConditionNode nanti)
public record FilterNode(
    PlanNode child,
    Object predicate
) implements PlanNode {
    @Override public List<PlanNode> getChildren() { return List.of(child); }
}