package com.apacy.common.dto;

import java.util.List;

/**
 * Dihasilkan oleh QO. Ini adalah representasi query string 
 * dalam bentuk OBJEK (Abstract Syntax Tree).
 * Object WhereConditionNode harus didefinisikan di tempat lain (misal: di QO internal).
 */
public record ParsedQuery(
    String queryType, // SELECT, UPDATE, CREATE, ...
    List<String> targetTables,
    List<String> targetColumns,
    Object joinConditions, // Bisa jadi record/class sendiri
    Object whereClause,    // Sebaiknya merujuk ke class/record AST internal QO
    String orderByColumn,
    boolean isDescending,
    boolean isOptimized
) {}