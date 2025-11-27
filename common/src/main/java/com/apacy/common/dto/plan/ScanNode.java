package com.apacy.common.dto.plan;

import java.util.List;

public record ScanNode(
    String tableName,
    String alias
) implements PlanNode {
    private static final long serialVersionUID = 1L;
    
    @Override public List<PlanNode> getChildren() { return List.of(); }
}