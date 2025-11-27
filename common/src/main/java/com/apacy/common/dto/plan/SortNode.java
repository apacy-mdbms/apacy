package com.apacy.common.dto.plan;

import java.util.List;

public record SortNode(
    PlanNode child,
    String sortColumn,
    boolean ascending
) implements PlanNode {
    private static final long serialVersionUID = 1L;
    
    @Override public List<PlanNode> getChildren() { return List.of(child); }
}