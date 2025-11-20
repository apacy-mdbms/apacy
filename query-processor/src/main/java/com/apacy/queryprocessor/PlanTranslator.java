package com.apacy.queryprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.ddl.ColumnDefinition;
import com.apacy.common.dto.ddl.ParsedQueryCreate;
import com.apacy.common.enums.IndexType;

/**
 * Translates ParsedQuery objects into specific operation DTOs.
 * TODO: Implement translation from abstract query plans to concrete execution operations
 */
public class PlanTranslator {
    
    /**
     * Translate a ParsedQuery to a DataRetrieval object.
     * @param parsedQuery The parsed query from Query Optimizer
     * @param transactionId Transaction ID for logging
     * @return DataRetrieval DTO for Storage Manager
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
     * Handles both INSERT and UPDATE operations.
     * 
     * @param parsedQuery The parsed query from Query Optimizer
     * @param transactionId Transaction ID for logging
     * @param isUpdate true for UPDATE, false for INSERT
     * @return DataWrite DTO for Storage Manager
     */
    public DataWrite translateToWrite(ParsedQuery parsedQuery, String transactionId, boolean isUpdate) {
        // Validasi input
        if (parsedQuery.targetTables() == null || parsedQuery.targetTables().isEmpty()) {
            throw new IllegalArgumentException("ParsedQuery untuk INSERT/UPDATE tidak memiliki target tabel.");
        }
        
        
        // 1. Ambil nama tabel
        String tableName = parsedQuery.targetTables().get(0);
        
        // 2. Ekstrak data untuk INSERT/UPDATE
        // TODO: ParsedQuery perlu field untuk menyimpan data (Map<String, Object>)
        // Untuk M1, kita buat placeholder yang bisa diisi nanti
        List<String> columns = parsedQuery.targetColumns();
        Map<String, Object> dataMap;
        
        if (columns == null || columns.isEmpty()) {
            // Placeholder kosong untuk testing
            dataMap = Map.of();
        } else {
            // TODO: Ambil values dari ParsedQuery (perlu field baru di ParsedQuery)
            // Untuk sekarang buat placeholder dengan null values
            dataMap = Map.of();
            // Contoh kalau ada data: dataMap = Map.of("name", "John", "age", 25);
        }
        
        Row newData = new Row(dataMap);
        
        // 3. Ambil filter condition (untuk UPDATE, ini WHERE clause)
        Object filter = parsedQuery.whereClause();
        
        // Untuk INSERT, filter biasanya null (tidak ada WHERE)
        // Untuk UPDATE, filter harus ada (WHERE clause menentukan row mana yang diupdate)
        if (isUpdate && filter == null) {
            // UPDATE tanpa WHERE akan update semua rows (dangerous, tapi valid SQL)
            // Bisa dikasih warning atau throw exception tergantung policy
            System.out.println("[WARNING] UPDATE query tanpa WHERE clause akan mengubah semua rows!");
        }
        
        // 4. Return DTO
        return new DataWrite(tableName, newData, filter);
    }
    
    /**
     * Translate a ParsedQuery to a DataDeletion object.
     * Handles DELETE operations.
     * 
     * @param parsedQuery The parsed query from Query Optimizer
     * @param transactionId Transaction ID for logging
     * @return DataDeletion DTO for Storage Manager
     */
    public DataDeletion translateToDeletion(ParsedQuery parsedQuery, String transactionId) {
        // Validasi input
        if (parsedQuery.targetTables() == null || parsedQuery.targetTables().isEmpty()) {
            throw new IllegalArgumentException("ParsedQuery untuk DELETE tidak memiliki target tabel.");
        }
        
        // Validasi queryType
        if (!"DELETE".equalsIgnoreCase(parsedQuery.queryType())) {
            throw new IllegalArgumentException(
                "translateToDeletion hanya untuk DELETE query, dapat: " + parsedQuery.queryType()
            );
        }
        
        // 1. Ambil nama tabel
        String tableName = parsedQuery.targetTables().get(0);
        
        // 2. Ambil filter condition (WHERE clause)
        Object filter = parsedQuery.whereClause();
        
        // DELETE tanpa WHERE akan menghapus semua rows (dangerous!)
        if (filter == null) {
            System.out.println("[WARNING] DELETE query tanpa WHERE clause akan menghapus SEMUA rows!");
            throw new IllegalArgumentException("DELETE tanpa WHERE tidak diizinkan");
        }
     
        return new DataDeletion(tableName, filter);
    }

    /**
     * Translate DTO Parser (ParsedQueryCreate) to DTO Storage (Schema).
     */
    public Schema translateToSchema(ParsedQueryCreate query) {
        String tableName = query.getTableName();
        String dataFileName = tableName + ".dat";

        List<Column> smColumns = new ArrayList<>();
        List<IndexSchema> smIndexes = new ArrayList<>();

        for (ColumnDefinition colDef : query.getColumns()) {
            Column col = new Column(colDef.getName(), colDef.getType(), colDef.getLength());
            smColumns.add(col);

            if (colDef.isPrimaryKey()) {
                String indexName = "pk_" + tableName + "_" + colDef.getName();
                String indexFile = tableName + "_" + colDef.getName() + ".idx";
                
                IndexSchema pkIndex = new IndexSchema(indexName, colDef.getName(), IndexType.Hash, indexFile);
                smIndexes.add(pkIndex);
            }
        }

        return new Schema(
            tableName, 
            dataFileName, 
            smColumns, 
            smIndexes, 
            query.getForeignKeys() 
        );
    }
}