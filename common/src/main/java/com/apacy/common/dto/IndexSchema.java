package com.apacy.common.dto;
import com.apacy.common.enums.*;

/**
 * Record (DTO) untuk menampung metadata satu indeks.
 */
public record IndexSchema(
    String indexName,  // misal: "idx_students_id"
    String columnName, // misalin: "StudentID"
    IndexType indexType,     // misal: 1=HASH, 2=BTREE
    String indexFile   // misal: "students_id.idx"
) {}