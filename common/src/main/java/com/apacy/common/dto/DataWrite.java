package com.apacy.common.dto;

import java.util.Map;

/**
 * Dibuat oleh QP, dikonsumsi oleh SM.
 * Instruksi spesifik ke SM tentang data apa yang harus DITULIS/DIUBAH.
 */
public record DataWrite(
    String tableName,
    Row newData,
    Object filterCondition // Sebaiknya merujuk ke class/record internal SM
) {}