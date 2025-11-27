package com.apacy.common.dto.plan;

import java.util.List;

/**
 * Merepresentasikan klausa LIMIT.
 * Membatasi jumlah baris yang mengalir ke atas pohon.
 */
public record LimitNode(
    PlanNode child,
    int limit
) implements PlanNode {
    private static final long serialVersionUID = 1L;

    @Override 
    public List<PlanNode> getChildren() { 
        return List.of(child); 
    }

    @Override 
    public String toString() { 
        return "Limit(" + limit + ")"; 
    }
}