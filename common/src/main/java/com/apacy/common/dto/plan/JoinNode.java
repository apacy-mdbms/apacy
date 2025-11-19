package com.apacy.common.dto.plan;

import java.util.List;

public record JoinNode(
    PlanNode left,
    PlanNode right,
    Object joinCondition,
    String joinType // INNER, LEFT, etc.
) implements PlanNode {
    @Override public List<PlanNode> getChildren() { return List.of(left, right); }
}
