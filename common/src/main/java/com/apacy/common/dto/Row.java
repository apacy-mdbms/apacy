package com.apacy.common.dto;

import java.util.Map;

/**
 * Merepresentasikan SATU baris data.
 * Ini adalah 'record' tapi memiliki helper method 'get' untuk kemudahan.
 */
public record Row(Map<String, Object> data) {
    
    /**
     * Helper method untuk mengambil nilai kolom berdasarkan nama.
     * @param columnName Nama kolom yang datanya ingin diambil.
     * @return Nilai dari kolom tersebut (bisa null).
     */
    public Object get(String columnName) {
        return data.get(columnName);
    }
}