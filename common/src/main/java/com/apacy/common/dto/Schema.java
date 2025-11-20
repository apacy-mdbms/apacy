package com.apacy.common.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Representasi skema DAN metadata tabel (kumpulan kolom,
 * nama file, dan indeks).
 */
public record Schema(
    String tableName,          // Nama tabel (e.g., "students")
    String dataFile,           // Lokasi file data (e.g., "students.dat")
    List<Column> columns,      // Daftar kolom
    List<IndexSchema> indexes, // Daftar indeks
    List<ForeignKeySchema> foreignKeys // Daftar Foreign Keys
) {

    // Constructor overload untuk backward compatibility
    public Schema(String tableName, String dataFile, List<Column> columns, List<IndexSchema> indexes) {
        this(tableName, dataFile, columns, indexes, new ArrayList<>());
    }

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

    // Helper untuk mengambil FK
    public List<ForeignKeySchema> getForeignKeys() {
        return foreignKeys == null ? new ArrayList<>() : foreignKeys;
    }
}