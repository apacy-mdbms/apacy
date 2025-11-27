package com.apacy.common.dto.plan;

import java.util.List;

// Menggunakan Object condition (bisa diganti AST WhereConditionNode nanti)
public record FilterNode(
    PlanNode child,
    Object predicate
) implements PlanNode {
    private static final long serialVersionUID = 1L;
    
    @Override public List<PlanNode> getChildren() { return List.of(child); }
}