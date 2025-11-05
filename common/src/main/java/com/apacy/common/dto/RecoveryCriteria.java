package com.apacy.common.dto;

import java.time.LocalDateTime;

/**
 * Data Transfer Object untuk kriteria recovery (VERSI DIPERSIMPEL).
 * Kriteria utama adalah "transaction ID" (untuk UNDO transaksi)
 * atau "timestamp" (untuk Point-in-Time Recovery, sebuah fitur bonus).
 * Field seperti logPosition, checkpointTime, dll. dihapus karena itu adalah
 * detail implementasi internal dari Failure Recovery Manager, bukan 
 * 'kriteria' yang dikirim oleh Query Processor.
 * * @param recoveryType Tipe recovery yang diminta, misal "UNDO_TRANSACTION" atau "POINT_IN_TIME".
 * @param transactionId ID transaksi yang ingin di-UNDO. (Hanya diisi jika type="UNDO_TRANSACTION")
 * @param targetTime Waktu target untuk Point-in-Time Recovery. (Hanya diisi jika type="POINT_IN_TIME")
 */
public record RecoveryCriteria(
    String recoveryType,
    String transactionId,   // Nullable
    LocalDateTime targetTime  // Nullable
) {}