package com.apacy.common.dto.plan;

import java.util.List;

/**
 * Node pembungkus untuk perintah DDL (CREATE, DROP, ALTER).
 * Tidak punya child. Tidak akan dioptimasi.
 */
public record DDLNode(
    String operation, // "CREATE_TABLE", "DROP_TABLE"
    String tableName,
    List<Object> columnDefinitions
) implements PlanNode {
    @Override public List<PlanNode> getChildren() { return List.of(); }
    @Override public String toString() { return "DDL: " + operation + " " + tableName; }
}