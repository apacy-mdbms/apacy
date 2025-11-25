package com.apacy.common.dto.plan;

import java.util.List;

/**
 * Merepresentasikan operasi modifikasi data (INSERT, UPDATE, DELETE).
 * Node ini biasanya menjadi Root dari query non-SELECT.
 */
public record ModifyNode(
    String operation,           // "INSERT", "UPDATE", atau "DELETE"
    PlanNode child,             // Sumber data (Scan/Filter utk DELETE/UPDATE, null utk INSERT VALUES biasa)
    String targetTable,         // Nama tabel yang akan dimodifikasi
    List<String> targetColumns, // Kolom target (diisi untuk INSERT/UPDATE, null untuk DELETE)
    List<Object> values         // Nilai konstan (diisi untuk INSERT VALUES/UPDATE SET, null untuk DELETE)
) implements PlanNode {

    @Override 
    public List<PlanNode> getChildren() { 
        // Child bisa null jika ini adalah INSERT ... VALUES sederhana
        return child != null ? List.of(child) : List.of(); 
    }

    @Override 
    public String toString() { 
        return "Modify(" + operation + " on " + targetTable + ")"; 
    }
}