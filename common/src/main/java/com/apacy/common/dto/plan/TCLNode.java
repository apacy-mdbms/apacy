package com.apacy.common.dto.plan;

import java.util.List;

/**
 * Node pembungkus untuk Transaction Control (BEGIN, COMMIT, ROLLBACK).
 */
public record TCLNode(
    String command // "BEGIN", "COMMIT", "ROLLBACK"
) implements PlanNode {
    @Override public List<PlanNode> getChildren() { return List.of(); }
}