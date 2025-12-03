package com.apacy.common.dto;

import java.util.Map;
import com.apacy.common.enums.IndexType;

/**
 * Berisi metadata statistik (nr, br, lr, fr, V(A,r), indexedColumn) 
 * yang dibutuhkan QO untuk menghitung biaya.
 */
public record Statistic(
    int nr, // jumlah tuple
    int br, // jumlah blok
    int lr, // ukuran tuple
    int fr, // blocking factor
    Map<String, Integer> V, // V(A,r): jumlah nilai distinct untuk atribut A
    Map<String, IndexType> indexedColumn, // Misal: {"user_id": Hash, "gpa": BPlusTree}
    Map<String, Object> minVal, 
    Map<String, Object> maxVal
) {
    public Statistic(int nr, int br, int lr, int fr, Map<String, Integer> V, Map<String, IndexType> indexedColumn) {
        this(nr, br, lr, fr, V, indexedColumn, Map.of(), Map.of());
    }
}

