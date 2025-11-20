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
        // 1. Buat objek DataRetrieval
        // columns = null artinya "SELECT *" (ambil semua kolom)
        // filterCondition = null karena filtering dilakukan oleh FilterNode di layer atasnya
        DataRetrieval dr = new DataRetrieval(
            node.tableName(), 
            null, 
            null, 
            false
        );

        // 2. Panggil sm.readBlock(dataRetrieval)
        return sm.readBlock(dr);
    }

    /**
     * [FARREL]
     * Menangani FilterNode: Menyaring baris berdasarkan kondisi WHERE.
     */
    public List<Row> executeFilter(FilterNode node, Function<PlanNode, List<Row>> childExecutor) {
        // 1. Ambil data dari anak
        List<Row> input = childExecutor.apply(node.child());
        List<Row> result = new ArrayList<>();

        // 2. Lakukan filtering menggunakan ConditionEvaluator
        for (Row row : input) {
            if (ConditionEvaluator.evaluate(row, node.predicate())) {
                result.add(row);
            }
        }

        // 3. Return baris yang lolos filter
        return result;
    }

    /**
     * [FARREL]
     * Menangani ProjectNode: Memilih kolom tertentu (SELECT col1, col2).
     */
    public List<Row> executeProject(ProjectNode node, Function<PlanNode, List<Row>> childExecutor) {
        // 1. Ambil data dari anak
        List<Row> input = childExecutor.apply(node.child());
        List<Row> result = new ArrayList<>();
        List<String> targetCols = node.columns();

        // 2. Lakukan mapping: Buat Row baru yang isinya HANYA kolom di node.columns()
        for (Row oldRow : input) {
            Map<String, Object> newMap = new HashMap<>();
            for (String col : targetCols) {
                newMap.put(col, oldRow.get(col));
            }
            result.add(new Row(newMap));
        }
        return result;
    }

    /**
     * [FARREL]
     * Menangani JoinNode: Menggabungkan dua tabel/node.
     */
    public List<Row> executeJoin(JoinNode node, Function<PlanNode, List<Row>> childExecutor, 
                                 JoinStrategy joinStrategy, int txId, IConcurrencyControlManager ccm) {
        // 1. Eksekusi anak kiri: List<Row> left = childExecutor.apply(node.left());
        // 2. Eksekusi anak kanan: List<Row> right = childExecutor.apply(node.right());
        // 3. Panggil JoinStrategy (misal: nestedLoopJoin)

        // 1. Eksekusi anak kiri dan kanan
        List<Row> leftRows = childExecutor.apply(node.left());
        List<Row> rightRows = childExecutor.apply(node.right());
        List<Row> result = new ArrayList<>();

        // 2. Logika Join

        for (Row left : leftRows) {
            for (Row right : rightRows) {
                // Gabungkan data kiri + kanan
                Map<String, Object> mergedData = new HashMap<>(left.data());
                // Strategi overwrite sederhana jika ada nama kolom sama
                mergedData.putAll(right.data()); 
                Row mergedRow = new Row(mergedData);

                // Evaluasi kondisi join
                if (ConditionEvaluator.evaluate(mergedRow, node.joinCondition())) {
                    result.add(mergedRow);
                }
            }
        }
        
        return result;
    }

    /**
     * [FARREL]
     * Menangani ModifyNode: Operasi INSERT, UPDATE, DELETE.
     */
    public List<Row> executeModify(ModifyNode node, int txId, Function<PlanNode, List<Row>> childExecutor,
                                   IStorageManager sm, IConcurrencyControlManager ccm, IFailureRecoveryManager frm) {
        // 1. Translate ModifyNode ke DTO storage (DataWrite / DataDeletion)
        // 2. Panggil sm.writeBlock atau sm.deleteBlock
        // 3. Buat Row dummy berisi info "affected_rows" untuk dikembalikan
        int affectedRows = 0;
        String operation = node.operation().toUpperCase();

        if ("INSERT".equals(operation)) {
            // INSERT: Gabungkan columns dan values menjadi Row
            Map<String, Object> dataMap = new HashMap<>();
            List<String> cols = node.targetColumns();
            List<Object> vals = node.values();

            if (cols != null && vals != null && cols.size() == vals.size()) {
                for (int i = 0; i < cols.size(); i++) {
                    dataMap.put(cols.get(i), vals.get(i));
                }
            }
            // Buat DataWrite
            DataWrite dw = new DataWrite(node.targetTable(), new Row(dataMap), null);
            affectedRows = sm.writeBlock(dw);

        } else if ("DELETE".equals(operation)) {
            // DELETE: Ambil kondisi WHERE dari child node (biasanya FilterNode)
            Object filterCondition = null;
            // Cek apakah child adalah FilterNode
            if (!node.getChildren().isEmpty() && node.getChildren().get(0) instanceof FilterNode fn) {
                filterCondition = fn.predicate();
            }

            // Buat DataDeletion
            DataDeletion dd = new DataDeletion(node.targetTable(), filterCondition);
            affectedRows = sm.deleteBlock(dd);

        } else if ("UPDATE".equals(operation)) {
            // UPDATE: mapping values +  ambil kondisi
            Map<String, Object> dataMap = new HashMap<>();
            List<String> cols = node.targetColumns();
            List<Object> vals = node.values();

            if (cols != null && vals != null) {
                for (int i = 0; i < cols.size(); i++) {
                    dataMap.put(cols.get(i), vals.get(i));
                }
            }

            Object filterCondition = null;
            if (!node.getChildren().isEmpty() && node.getChildren().get(0) instanceof FilterNode fn) {
                filterCondition = fn.predicate();
            }

            DataWrite dw = new DataWrite(node.targetTable(), new Row(dataMap), filterCondition);
            affectedRows = sm.writeBlock(dw);
        }

        // 3. Buat Row dummy berisi info "affected_rows"
        Map<String, Object> resultData = new HashMap<>();
        resultData.put("affected_rows", affectedRows);
        return List.of(new Row(resultData));
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