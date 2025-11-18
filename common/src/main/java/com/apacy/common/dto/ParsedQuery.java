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
    boolean isOptimized
) {
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
                    isDescending,
                    isOptimized
                    );
    }
}
