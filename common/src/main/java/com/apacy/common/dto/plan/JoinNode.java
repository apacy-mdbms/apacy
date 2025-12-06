package com.apacy.common.dto.plan;

import java.util.List;
import com.apacy.common.enums.JoinAlgorithm

public record JoinNode(
    PlanNode left,
    PlanNode right,
    Object joinCondition, // PAKAI WhereConditionNode, sebagai theta
    String joinType, // INNER, LEFT, etc.
    JoinAlgorithm algorihtm
) implements PlanNode {
    private static final long serialVersionUID = 1L;

    public JoinNode(PlanNode left, PlanNode right, Object joinCondition, String joinType) {
        this(left, right, joinCondition, joinType, null);
    }
    
    @Override public List<PlanNode> getChildren() { return List.of(left, right); }
}
