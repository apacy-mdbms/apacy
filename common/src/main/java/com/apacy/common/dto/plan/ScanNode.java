package com.apacy.common.dto.plan;

import java.util.List;

public record ScanNode(
    String tableName,
    String alias
) implements PlanNode {
    @Override public List<PlanNode> getChildren() { return List.of(); }
}