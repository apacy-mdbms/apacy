package com.apacy.common.dto;

import java.util.Map;

/**
 * Berisi metadata statistik (nr, br, lr, fr, V(A,r)) 
 * yang dibutuhkan QO untuk menghitung biaya.
 */
public record Statistic(
    int nr, // jumlah tuple
    int br, // jumlah blok
    int lr, // ukuran tuple
    int fr, // blocking factor
    Map<String, Integer> V // V(A,r): jumlah nilai distinct untuk atribut A
) {}