package com.apacy.common.dto;

/**
 * Dibuat oleh QP, dikonsumsi oleh SM.
 * Instruksi spesifik ke SM tentang data apa yang harus DIHAPUS.
 */
public record DataDeletion(
    String tableName,
    Object filterCondition // Sebaiknya merujuk ke class/record internal SM
) {}