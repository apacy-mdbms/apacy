package com.apacy.common.dto;

import java.util.List;

import com.apacy.common.dto.plan.PlanNode;

/**
 * Dihasilkan oleh QO. Ini adalah representasi query string
 * dalam bentuk OBJEK (Abstract Syntax Tree).
 * Object WhereConditionNode harus didefinisikan di tempat lain (misal: di QO internal).
 */
public record ParsedQuery(
    String queryType, // SELECT, UPDATE, CREATE, ...
    PlanNode planRoot,
    List<String> targetTables,
    List<String> targetColumns,
    List<Object> values, // write values buat INSERT / UPDATE

    // LEGACY
    Object joinConditions, // Bisa jadi record/class sendiri
    Object whereClause,    // Sebaiknya merujuk ke class/record AST internal QO
    String orderByColumn,
    boolean isDescending,
    boolean isOptimized,
    Integer limit,
    Integer offset
) {
    // --- KONSTRUKTOR UNTUK BACKWARD COMPATIBILITY ---
    // Ini agar kode lama/test lain tidak error karena jumlah argumen berubah.
    public ParsedQuery(
            String queryType,
            PlanNode planRoot,
            List<String> targetTables,
            List<String> targetColumns,
            List<Object> values,
            Object joinConditions,
            Object whereClause,
            String orderByColumn,
            boolean isDescending,
            boolean isOptimized
    ) {
        // Panggil konstruktor utama dengan limit & offset = null
        this(queryType, planRoot, targetTables, targetColumns, values, joinConditions, whereClause, orderByColumn, isDescending, isOptimized, null, null);
    }
    
    @Override
    public String toString() {
        return """
        Query: %s
        Tables: %s
        Columns: %s
        Values: %s
        Join: %s
        Where: %s
        OrderBy: %s
        isDescending: %b
        isOptimized: %b

                """.formatted(
                    queryType,
                    targetTables != null ? targetTables.toString() : "none",
                    targetColumns != null ? targetColumns.toString() : "none",
                    values != null ? values.toString() : "none",
                    joinConditions != null ? joinConditions.toString() : "none",
                    whereClause != null ? whereClause.toString() : "none",
                    orderByColumn,
                    limit != null ? limit : "none",
                    offset != null ? offset : "none",
                    isDescending,
                    isOptimized
                    );
    }
}
