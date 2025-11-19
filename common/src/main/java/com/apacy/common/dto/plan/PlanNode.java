package com.apacy.common.dto.plan;

import java.util.List;

// Interface dasar untuk semua operasi aljabar
public sealed interface PlanNode
    permits ProjectNode, FilterNode, JoinNode, ScanNode, SortNode, ModifyNode, LimitNode, TCLNode, DDLNode {

    List<PlanNode> getChildren();
}