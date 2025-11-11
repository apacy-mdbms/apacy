package com.apacy.common.dto;

import java.util.List;

/**
 * Representasi skema DAN metadata tabel (kumpulan kolom,
 * nama file, dan indeks).
 */
public record Schema(
    String tableName,          // Nama tabel (e.g., "students")
    String dataFile,           // Lokasi file data (e.g., "students.dat")
    List<Column> columns,      // Daftar kolom
    List<IndexSchema> indexes  // Daftar indeks
) {

    /**
     * Helper untuk mengambil kolom berdasarkan indeks.
     */
    public Column getColumn(int index) {
        return columns.get(index);
    }

    /**
     * Helper untuk mendapatkan jumlah kolom.
     */
    public int getColumnCount() {
        return columns.size();
    }
    
    /**
     * Helper untuk mencari skema kolom berdasarkan nama.
     */
    public Column getColumnByName(String name) {
        for (Column col : columns) {
            if (col.name().equalsIgnoreCase(name)) {
                return col;
            }
        }
        return null; // Atau lempar exception
    }
}