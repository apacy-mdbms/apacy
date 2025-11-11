package com.apacy.common.dto; // (Atau package Anda)
import com.apacy.common.enums.DataType;

/**
 * Representasi satu kolom dalam skema.
 * (Versi record yang immutable)
 */
public record Column(
    String name,
    DataType type,
    int length // 0 untuk INT/FLOAT, >0 untuk CHAR/VARCHAR
) {
    
    /**
     * Konstruktor tambahan untuk tipe data non-string (INT/FLOAT)
     * yang tidak memiliki 'length'.
     */
    public Column(String name, DataType type) {
        this(name, type, 0);
    }

}