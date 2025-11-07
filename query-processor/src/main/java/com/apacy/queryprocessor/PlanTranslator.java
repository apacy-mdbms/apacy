package com.apacy.queryprocessor;

import com.apacy.common.dto.*;
import java.util.List;

/**
 * Translates ParsedQuery objects into specific operation DTOs.
 * TODO: Implement translation from abstract query plans to concrete execution operations
 */
public class PlanTranslator {
    
    /**
     * Translate a ParsedQuery to a DataRetrieval object.
     * TODO: Implement translation for SELECT queries with proper predicate and projection handling
     * // SORI ini gue implementasi buat testing, nanti lanjutin/perbaikin aja
     */
    public DataRetrieval translateToRetrieval(ParsedQuery parsedQuery, String transactionId) {
        if (parsedQuery.targetTables() == null || parsedQuery.targetTables().isEmpty()) {
            throw new IllegalArgumentException("ParsedQuery untuk SELECT tidak memiliki target tabel.");
        }
        
        // 1. Ambil tabel pertama (untuk M1, ini satu-satunya tabel)
        String tableName = parsedQuery.targetTables().get(0);
        
        // 2. Ambil daftar kolom yang diminta
        List<String> columns = parsedQuery.targetColumns();
        
        // 3. Ambil AST 'whereClause'. Ini adalah 'Object' yang akan kita teruskan
        // ke SM. SM (atau filter di QP) harus bisa menginterpretasi ini.
        // Kita perlu cast ke WhereConditionNode (atau Object)
        Object filter = parsedQuery.whereClause();
        
        // 4. Cek apakah QO menyarankan pakai index.
        // Asumsi sederhana: jika QO menandai 'isOptimized', kita coba pakai index.
        // (Logika lebih canggih bisa membaca AST untuk 'id = 5')
        boolean useIndex = parsedQuery.isOptimized();

        // 5. Buat dan kembalikan DTO DataRetrieval
        return new DataRetrieval(tableName, columns, filter, useIndex);
    }
    
    /**
     * Translate a ParsedQuery to a DataWrite object.
     * TODO: Implement translation for INSERT/UPDATE queries with data mapping
     */
    public DataWrite translateToWrite(ParsedQuery parsedQuery, String transactionId, boolean isUpdate) {
        // TODO: Translate INSERT/UPDATE query to DataWrite DTO
        throw new UnsupportedOperationException("translateToWrite not implemented yet");
    }
    
    /**
     * Translate a ParsedQuery to a DataDeletion object.
     * TODO: Implement translation for DELETE queries with condition handling
     */
    public DataDeletion translateToDeletion(ParsedQuery parsedQuery, String transactionId) {
        // TODO: Translate DELETE query to DataDeletion DTO
        throw new UnsupportedOperationException("translateToDeletion not implemented yet");
    }
}