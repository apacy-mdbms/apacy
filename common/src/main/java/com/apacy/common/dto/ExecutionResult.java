package com.apacy.common.dto;

import java.io.Serializable;
import java.util.List;

/**
 * Dihasilkan oleh QP, dikonsumsi oleh FR (dan ditampilkan ke user).
 * Laporan akhir sebuah eksekusi query. Berisi info untuk log.
 */
public record ExecutionResult(
    boolean success,
    String message,
    int transactionId,
    String operation,
    int affectedRows,
    List<Row> rows // Untuk menampung hasil SELECT
) implements Serializable {
    private static final long serialVersionUID = 1L;
}