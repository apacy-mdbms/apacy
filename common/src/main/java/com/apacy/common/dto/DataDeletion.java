package com.apacy.common.dto;

import com.apacy.common.dto.ast.where.WhereConditionNode;

/**
 * Dibuat oleh QP, dikonsumsi oleh SM.
 * Instruksi spesifik ke SM tentang data apa yang harus DIHAPUS.
 */
public record DataDeletion(
    String tableName,
    WhereConditionNode filterCondition // Sebaiknya merujuk ke class/record internal SM
) {}