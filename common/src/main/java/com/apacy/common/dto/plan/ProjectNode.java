package com.apacy.common.dto.plan;

import java.util.List;

public record ProjectNode(
    PlanNode child,
    List<String> columns
) implements PlanNode {
    @Override public List<PlanNode> getChildren() { return List.of(child); }
}