package com.apacy.common.enums;

/**
 * Enum sederhana. Dibuat oleh QP, dikonsumsi oleh CCM.
 * Menandakan aksi fundamental yang diminta: BACA atau TULIS.
 * UPDATE/DELETE/INSERT/ALTER/CREATE semuanya adalah turunan dari 'WRITE'.
 */
public enum Action {
    READ,
    WRITE
}
