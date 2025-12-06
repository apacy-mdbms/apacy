package com.apacy.common.dto;

import java.util.List;

import com.apacy.common.dto.ast.where.WhereConditionNode;

/**
 * Dibuat oleh QP, dikonsumsi oleh SM.
 * Instruksi spesifik ke SM tentang data apa yang harus DIAMBIL.
 */
public record DataRetrieval(
    String tableName,
    List<String> columns,
    WhereConditionNode filterCondition, // Sebaiknya merujuk ke class/record internal SM
    boolean useIndex // perlu ubah jadi Map<String,IndexType>? 
) {}