package com.apacy.common.dto;

/**
 * DTO untuk menyimpan metadata Foreign Key.
 * Contoh: FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE
 */
public record ForeignKeySchema(
    String constraintName,      // misal: "fk_grades_student_id"
    String columnName,          // misal: "student_id" (kolom di tabel ini)
    String referenceTable,      // misal: "students" (tabel tujuan)
    String referenceColumn,     // misal: "id" (kolom di tabel tujuan)
    boolean isCascading         // true = ON DELETE CASCADE, false = RESTRICT
) {}