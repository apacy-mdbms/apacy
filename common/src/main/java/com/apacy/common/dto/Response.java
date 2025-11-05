package com.apacy.common.dto;

import java.util.Map;

/**
 * Dihasilkan oleh CCM, dikonsumsi oleh QP.
 * Jawaban "YA" atau "TIDAK" dari CCM atas permintaan aksi.
 */
public record Response(
    boolean isAllowed,
    String reason
) {}