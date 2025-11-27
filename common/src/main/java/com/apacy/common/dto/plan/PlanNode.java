package com.apacy.common.dto.plan;

import java.io.Serializable;
import java.util.List;

// Interface dasar untuk semua operasi aljabar
public sealed interface PlanNode extends Serializable
    permits ProjectNode, FilterNode, JoinNode, CartesianNode, ScanNode, SortNode, ModifyNode, LimitNode, TCLNode, DDLNode {

    List<PlanNode> getChildren();
}