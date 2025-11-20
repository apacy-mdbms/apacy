package com.apacy.queryprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.ddl.ColumnDefinition;
import com.apacy.common.dto.ddl.ParsedQueryCreate;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.dto.ddl.ParsedQueryDrop;
import com.apacy.common.dto.plan.DDLNode;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.LimitNode;
import com.apacy.common.dto.plan.ModifyNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.dto.plan.SortNode;
import com.apacy.common.dto.plan.TCLNode;
import com.apacy.common.enums.IndexType;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.execution.SortStrategy;

/**
 * PlanTranslator sekarang bertindak sebagai "Node Executor Implementation".
 * Kelas ini berisi logika detail bagaimana setiap PlanNode dijalankan.
 */
public class PlanTranslator {

    // ==================================================================================
    // [BAGIAN 1] CORE DATA PROCESSING
    // Penanggung Jawab: FARREL
    // Deskripsi: Menangani alur utama pengambilan, penggabungan, dan penyaringan data.
    // ==================================================================================

    /**
     * [FARREL]
     * Menangani ScanNode: Membaca data mentah dari Storage Manager.
     * Ini adalah titik awal (Leaf) dari pohon eksekusi.
     */
    public List<Row> executeScan(ScanNode node, int txId, IStorageManager sm, IConcurrencyControlManager ccm) {
        // TODO: Implementasi
        // 1. Buat objek DataRetrieval (gunakan node.tableName())
        // 2. Panggil sm.readBlock(dataRetrieval)
        // 3. Return List<Row> hasil bacaan
        return Collections.emptyList(); // Dummy return
    }

    /**
     * [FARREL]
     * Menangani FilterNode: Menyaring baris berdasarkan kondisi WHERE.
     */
    public List<Row> executeFilter(FilterNode node, Function<PlanNode, List<Row>> childExecutor) {
        // TODO: Implementasi
        // 1. Ambil data dari anak: List<Row> input = childExecutor.apply(node.child());
        // 2. Lakukan filtering menggunakan ConditionEvaluator
        // 3. Return baris yang lolos filter
        return Collections.emptyList(); // Dummy return
    }

    /**
     * [FARREL]
     * Menangani ProjectNode: Memilih kolom tertentu (SELECT col1, col2).
     */
    public List<Row> executeProject(ProjectNode node, Function<PlanNode, List<Row>> childExecutor) {
        // TODO: Implementasi
        // 1. Ambil data dari anak: List<Row> input = childExecutor.apply(node.child());
        // 2. Lakukan mapping: Buat Row baru yang isinya HANYA kolom di node.columns()
        return Collections.emptyList(); // Dummy return
    }

    /**
     * [FARREL]
     * Menangani JoinNode: Menggabungkan dua tabel/node.
     */
    public List<Row> executeJoin(JoinNode node, Function<PlanNode, List<Row>> childExecutor, 
                                 JoinStrategy joinStrategy, int txId, IConcurrencyControlManager ccm) {
        // TODO: Implementasi
        // 1. Eksekusi anak kiri: List<Row> left = childExecutor.apply(node.left());
        // 2. Eksekusi anak kanan: List<Row> right = childExecutor.apply(node.right());
        // 3. Panggil JoinStrategy (misal: nestedLoopJoin)
        return Collections.emptyList(); // Dummy return
    }

    /**
     * [FARREL]
     * Menangani ModifyNode: Operasi INSERT, UPDATE, DELETE.
     */
    public List<Row> executeModify(ModifyNode node, int txId, Function<PlanNode, List<Row>> childExecutor,
                                   IStorageManager sm, IConcurrencyControlManager ccm, IFailureRecoveryManager frm) {
        // TODO: Implementasi
        // 1. Translate ModifyNode ke DTO storage (DataWrite / DataDeletion)
        // 2. Panggil sm.writeBlock atau sm.deleteBlock
        // 3. Buat Row dummy berisi info "affected_rows" untuk dikembalikan
        return Collections.emptyList(); // Dummy return
    }


    // ==================================================================================
    // [BAGIAN 2] POST-PROCESSING & UTILITIES
    // Penanggung Jawab: WEKA
    // Deskripsi: Menangani pembentukan hasil akhir dan perintah utilitas.
    // ==================================================================================

    /**
     * [WEKA]
     * Menangani SortNode: Mengurutkan hasil (ORDER BY).
     */
    public List<Row> executeSort(SortNode node, Function<PlanNode, List<Row>> childExecutor, SortStrategy sortStrategy) {
        // TODO: Implementasi
        // 1. Ambil data dari anak
        // 2. Panggil SortStrategy.sort(rows, node.sortColumn(), node.ascending())
        return Collections.emptyList(); // Dummy return
    }

    /**
     * [WEKA]
     * Menangani LimitNode: Membatasi jumlah baris (LIMIT/OFFSET).
     */
    public List<Row> executeLimit(LimitNode node, Function<PlanNode, List<Row>> childExecutor) {
        // TODO: Implementasi
        // 1. Ambil data dari anak
        // 2. Gunakan logic subList() untuk skip (offset) dan limit
        return Collections.emptyList(); // Dummy return
    }

    /**
     * [WEKA]
     * Menangani TCLNode: Perintah Transaksi (BEGIN, COMMIT, ROLLBACK).
     */
    public List<Row> executeTCL(TCLNode node, IConcurrencyControlManager ccm, int txId) {
        // TODO: Implementasi
        // Panggil method di CCM (endTransaction, dll)
        // Return status sukses
        return Collections.emptyList(); // Dummy return
    }

    // ==================================================================================
    // [BAGIAN 3] DDL
    // Penanggung Jawab: KINAN
    // Deskripsi: Menangani pembentukan dan perubahan tabel.
    // ==================================================================================
    /**
     * [KINAN] - Load Balanced
     * Menangani DDLNode: CREATE TABLE, DROP TABLE, dll.
     */
    public List<Row> executeDDL(DDLNode node, IStorageManager sm) {
        ParsedQueryDDL ddlQuery = node.ddlQuery(); 

        try {
            // CREATE TABLE
            if (ddlQuery instanceof ParsedQueryCreate createCmd) {
                Schema schema = translateToSchema(createCmd);
                sm.createTable(schema);
            }
            
            // DROP TABLE
            else if (ddlQuery instanceof ParsedQueryDrop dropCmd) {
                // sm.dropTable(dropCmd.getTableName(), dropCmd.isCascading());
            }

            return Collections.emptyList();
        } catch (IOException e) {
            throw new RuntimeException("Storage IO Error executing DDL: " + e.getMessage(), e);
        }
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