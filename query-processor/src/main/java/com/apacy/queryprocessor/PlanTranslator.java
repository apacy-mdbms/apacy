package com.apacy.queryprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.ddl.ColumnDefinition;
import com.apacy.common.dto.ddl.ParsedQueryCreate;
import com.apacy.common.dto.ddl.ParsedQueryDDL;
import com.apacy.common.dto.ddl.ParsedQueryDrop;
import com.apacy.common.dto.plan.CartesianNode;
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
import com.apacy.queryprocessor.evaluator.ConditionEvaluator;
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

        // 2. Handle special case for "*" (select all columns)
        if (targetCols.size() == 1 && "*".equals(targetCols.get(0))) {
            // Return all rows as-is without projection
            return input;
        }

        // 3. Lakukan mapping: Buat Row baru yang isinya HANYA kolom di node.columns()
        for (Row oldRow : input) {
            Map<String, Object> newMap = new HashMap<>();
            for (String col : targetCols) {
                if (oldRow.data().containsKey(col)) {
                    newMap.put(col, oldRow.get(col));
                }
            }
            result.add(new Row(newMap));
        }
        return result;
    }

    /**
     * [FARREL]
     * Menangani JoinNode: Menggabungkan dua tabel/node.
     * Support untuk INNER, LEFT/RIGHT/FULL OUTER, CROSS, dan NATURAL JOIN.
     */
    public List<Row> executeJoin(JoinNode node, Function<PlanNode, List<Row>> childExecutor, 
                                 JoinStrategy joinStrategy, int txId, IConcurrencyControlManager ccm) {
        // Eksekusi anak kiri dan kanan
        List<Row> leftRows = childExecutor.apply(node.left());
        List<Row> rightRows = childExecutor.apply(node.right());
        
        // Tentukan join type (default: INNER)
        String joinType = node.joinType() != null ? node.joinType().toUpperCase() : "INNER";
        
        // Ekstrak join column dari condition (untuk equi-join sederhana)
        String joinColumn = extractJoinColumn(node.joinCondition());
        
        // Execute berdasarkan join type
        switch (joinType) {
            case "INNER":
                return executeInnerJoin(leftRows, rightRows, joinColumn, node.joinCondition());
                
            case "LEFT":
            case "LEFT OUTER":
                return executeLeftOuterJoin(leftRows, rightRows, joinColumn, node.joinCondition());
                
            case "RIGHT":
            case "RIGHT OUTER":
                return executeRightOuterJoin(leftRows, rightRows, joinColumn, node.joinCondition());
                
            case "FULL":
            case "FULL OUTER":
                return executeFullOuterJoin(leftRows, rightRows, joinColumn, node.joinCondition());
                
            case "CROSS":
                return executeCrossJoin(leftRows, rightRows);
                
            case "NATURAL":
                return executeNaturalJoin(leftRows, rightRows);
                
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
    }
    
    /**
     * INNER JOIN: Mengembalikan baris yang cocok di kedua tabel.
     * Menggunakan hash join untuk dataset besar, nested loop untuk dataset kecil.
     */
    private List<Row> executeInnerJoin(List<Row> leftRows, List<Row> rightRows, 
                                       String joinColumn, Object joinCondition) {
        if (joinColumn != null && !joinColumn.isEmpty()) {
            if (leftRows.size() * rightRows.size() > 1000) {
                // Hash join untuk dataset besar (cartesian product > 1000)
                return JoinStrategy.hashJoin(leftRows, rightRows, joinColumn);
            } else {
                // Nested loop join untuk dataset kecil
                return JoinStrategy.nestedLoopJoin(leftRows, rightRows, joinColumn);
            }
        } else {
            // Fallback: Manual evaluation untuk non-equi-join
            List<Row> result = new ArrayList<>();
            for (Row left : leftRows) {
                for (Row right : rightRows) {
                    Map<String, Object> mergedData = new HashMap<>(left.data());
                    mergedData.putAll(right.data());
                    Row mergedRow = new Row(mergedData);
                    
                    if (ConditionEvaluator.evaluate(mergedRow, joinCondition)) {
                        result.add(mergedRow);
                    }
                }
            }
            return result;
        }
    }
    
    /**
     * LEFT OUTER JOIN: Mengembalikan semua baris dari tabel kiri, dan baris yang cocok dari tabel kanan.
     * Baris kiri yang tidak cocok akan memiliki NULL untuk kolom kanan.
     */
    private List<Row> executeLeftOuterJoin(List<Row> leftRows, List<Row> rightRows,
                                           String joinColumn, Object joinCondition) {
        // gabungkan baris yang cocok
        List<Row> innerResult = executeInnerJoin(leftRows, rightRows, joinColumn, joinCondition);
        List<Row> result = new ArrayList<>(innerResult);
        
        // left rows yang tidak ada di inner result
        Set<Row> matchedLeftRows = new HashSet<>();
        for (Row innerRow : innerResult) {
            for (Row left : leftRows) {
                if (rowContainsData(innerRow, left)) {
                    matchedLeftRows.add(left);
                    break;
                }
            }
        }
        
        // Tambahkan baris kiri yang tidak cocok dengan NULL untuk kolom kanan
        for (Row left : leftRows) {
            if (!matchedLeftRows.contains(left)) {
                Map<String, Object> mergedData = new HashMap<>(left.data());
                
                // Tambahkan kolom kanan dengan nilai NULL
                if (!rightRows.isEmpty()) {
                    for (String key : rightRows.get(0).data().keySet()) {
                        mergedData.putIfAbsent(key, null);
                    }
                }
                
                result.add(new Row(mergedData));
            }
        }
        
        return result;
    }
    
    /**
     * RIGHT OUTER JOIN: Mengembalikan semua baris dari tabel kanan, dan baris yang cocok dari tabel kiri.
     * Baris kanan yang tidak cocok akan memiliki NULL untuk kolom kiri.
     */
    private List<Row> executeRightOuterJoin(List<Row> leftRows, List<Row> rightRows,
                                            String joinColumn, Object joinCondition) {
        // gabungkan baris yang cocok
        List<Row> innerResult = executeInnerJoin(leftRows, rightRows, joinColumn, joinCondition);
        List<Row> result = new ArrayList<>(innerResult);
        
        // right rows yang tidak ada di inner result
        Set<Row> matchedRightRows = new HashSet<>();
        for (Row innerRow : innerResult) {
            for (Row right : rightRows) {
                if (rowContainsData(innerRow, right)) {
                    matchedRightRows.add(right);
                    break;
                }
            }
        }
        
        // Tambahkan baris kanan yang tidak cocok dengan NULL untuk kolom kiri
        for (Row right : rightRows) {
            if (!matchedRightRows.contains(right)) {
                Map<String, Object> mergedData = new HashMap<>();
                
                // Tambahkan kolom kiri dengan nilai NULL
                if (!leftRows.isEmpty()) {
                    for (String key : leftRows.get(0).data().keySet()) {
                        mergedData.put(key, null);
                    }
                }
                
                // Tambahkan kolom kanan
                mergedData.putAll(right.data());
                
                result.add(new Row(mergedData));
            }
        }
        
        return result;
    }
    
    /**
     * FULL OUTER JOIN: Mengembalikan semua baris dari kedua tabel.
     * Baris yang tidak cocok akan memiliki NULL untuk kolom tabel lain.
     */
    private List<Row> executeFullOuterJoin(List<Row> leftRows, List<Row> rightRows,
                                           String joinColumn, Object joinCondition) {
        // Inner join
        List<Row> innerResult = executeInnerJoin(leftRows, rightRows, joinColumn, joinCondition);
        List<Row> result = new ArrayList<>(innerResult);
        
        // left and right rows that are not in inner result
        Set<Row> matchedLeftRows = new HashSet<>();
        Set<Row> matchedRightRows = new HashSet<>();
        
        for (Row innerRow : innerResult) {
            for (Row left : leftRows) {
                if (rowContainsData(innerRow, left)) {
                    matchedLeftRows.add(left);
                    break;
                }
            }
            for (Row right : rightRows) {
                if (rowContainsData(innerRow, right)) {
                    matchedRightRows.add(right);
                    break;
                }
            }
        }
        
        // Tambahkan baris kiri yang tidak cocok
        for (Row left : leftRows) {
            if (!matchedLeftRows.contains(left)) {
                Map<String, Object> mergedData = new HashMap<>(left.data());
                
                if (!rightRows.isEmpty()) {
                    for (String key : rightRows.get(0).data().keySet()) {
                        mergedData.putIfAbsent(key, null);
                    }
                }
                
                result.add(new Row(mergedData));
            }
        }
        
        // Tambahkan baris kanan yang tidak cocok
        for (Row right : rightRows) {
            if (!matchedRightRows.contains(right)) {
                Map<String, Object> mergedData = new HashMap<>();
                
                if (!leftRows.isEmpty()) {
                    for (String key : leftRows.get(0).data().keySet()) {
                        mergedData.put(key, null);
                    }
                }
                
                mergedData.putAll(right.data());
                
                result.add(new Row(mergedData));
            }
        }
        
        return result;
    }
    
    /**
     * CROSS JOIN: Mengembalikan cartesian product dari kedua tabel.
     */
    private List<Row> executeCrossJoin(List<Row> leftRows, List<Row> rightRows) {
        return JoinStrategy.cartesianJoin(leftRows, rightRows);
    }
    
    /**
     * NATURAL JOIN: Menggabungkan tabel berdasarkan kolom dengan nama yang sama.
     * Otomatis mendeteksi kolom yang sama dan melakukan equi-join.
     * Kolom yang sama hanya muncul sekali di hasil.
     */
    
    private List<Row> executeNaturalJoin(List<Row> leftRows, List<Row> rightRows) {
        if (leftRows.isEmpty() || rightRows.isEmpty()) {
            return Collections.emptyList();
        }

        // Common Columns
        Set<String> leftCols = leftRows.get(0).data().keySet();
        List<String> commonCols = rightRows.get(0).data().keySet().stream()
                .filter(leftCols::contains)
                .collect(Collectors.toList());

        // Jika tidak ada kolom sama menjadi CROSS JOIN (Cartesian)
        if (commonCols.isEmpty()) {
            return JoinStrategy.cartesianJoin(leftRows, rightRows);
        }

        // Pastikan JoinStrategy.hashJoin Anda sudah support List<String> (seperti diskusi sebelumnya)
        return JoinStrategy.hashJoin(leftRows, rightRows, commonCols);
    }
    
    /**
     * Ekstrak nama kolom join dari WhereConditionNode untuk equi-join sederhana.
     * Hanya support simple equi-join: table1.col = table2.col
     * Return null jika kondisi terlalu kompleks.
     */
    private String extractJoinColumn(Object condition) {
        if (!(condition instanceof com.apacy.queryoptimizer.ast.where.ComparisonConditionNode)) {
            return null;
        }
        
        com.apacy.queryoptimizer.ast.where.ComparisonConditionNode comp = 
            (com.apacy.queryoptimizer.ast.where.ComparisonConditionNode) condition;
        
        // Hanya support operator "="
        if (!"=".equals(comp.operator())) {
            return null;
        }
        
        // Ekstrak nama kolom dari left dan right operand
        String leftCol = extractColumnNameFromExpression(comp.leftOperand());
        String rightCol = extractColumnNameFromExpression(comp.rightOperand());
        
        // Jika kedua kolom valid dan sama namanya (tanpa table prefix), return nama kolom
        if (leftCol != null && rightCol != null && leftCol.equals(rightCol)) {
            return leftCol;
        }
        
        return null; // Kondisi kompleks atau kolom berbeda
    }
    
    /**
     * Ekstrak nama kolom dari ExpressionNode
     * Method ini mirip dengan extractColumnsFromExpression di DistributeProjectRewriter
     */
    private String extractColumnNameFromExpression(com.apacy.queryoptimizer.ast.expression.ExpressionNode expr) {
        if (expr == null || expr.term() == null) {
            return null;
        }
        
        // Ekstrak dari term utama
        String colName = extractColumnNameFromTerm(expr.term());
        if (colName != null) {
            return colName;
        }
        
        // Cek remainder terms jika ada
        if (expr.remainderTerms() != null && !expr.remainderTerms().isEmpty()) {
            for (var pair : expr.remainderTerms()) {
                colName = extractColumnNameFromTerm(pair.term());
                if (colName != null) {
                    return colName;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Ekstrak nama kolom dari TermNode
     */
    private String extractColumnNameFromTerm(com.apacy.queryoptimizer.ast.expression.TermNode term) {
        if (term == null || term.factor() == null) {
            return null;
        }
        
        // Cek factor utama
        if (term.factor() instanceof com.apacy.queryoptimizer.ast.expression.ColumnFactor colFactor) {
            String colName = colFactor.columnName();
            
            // Jika ada table prefix (table.column), ambil hanya column name
            if (colName.contains(".")) {
                String[] parts = colName.split("\\.");
                return parts[parts.length - 1]; // Ambil bagian terakhir
            }
            
            return colName;
        }
        
        // Cek remainder factors jika ada
        if (term.remainderFactors() != null && !term.remainderFactors().isEmpty()) {
            for (var pair : term.remainderFactors()) {
                if (pair.factor() instanceof com.apacy.queryoptimizer.ast.expression.ColumnFactor colFactor) {
                    String colName = colFactor.columnName();
                    
                    if (colName.contains(".")) {
                        String[] parts = colName.split("\\.");
                        return parts[parts.length - 1];
                    }
                    
                    return colName;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Helper: Cek apakah merged row mengandung data dari source row
     */
    private boolean rowContainsData(Row mergedRow, Row sourceRow) {
        for (Map.Entry<String, Object> entry : sourceRow.data().entrySet()) {
            Object mergedValue = mergedRow.get(entry.getKey());
            if (mergedValue == null && entry.getValue() != null) {
                return false;
            }
            if (mergedValue != null && !mergedValue.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public List<Row> executeCartesian(CartesianNode node, Function<PlanNode, List<Row>> childExecutor,
                                      int txId, IConcurrencyControlManager ccm) {
        // 1. Eksekusi anak kiri dan kanan
        List<Row> leftRows = childExecutor.apply(node.left());
        List<Row> rightRows = childExecutor.apply(node.right());
        
        // 2. Lakukan Cartesian Join via JoinStrategy
        return JoinStrategy.cartesianJoin(leftRows, rightRows);
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
        // 1. Ambil data dari anak
        List<Row> input = childExecutor.apply(node.child());
        
        // 2. Panggil SortStrategy.sort() secara static
        return SortStrategy.sort(input, node.sortColumn(), node.ascending());
    }

    /**
     * [WEKA] - IMPLEMENTED
     * Menangani LimitNode: Membatasi jumlah baris (LIMIT only, no OFFSET support).
     * 
     * Note: Current LimitNode only supports LIMIT, not OFFSET.
     * To add OFFSET support, update LimitNode record to include offset field.
     * 
     * @param node LimitNode yang berisi informasi limit
     * @param childExecutor Function untuk mengeksekusi child node
     * @return List<Row> yang sudah di-limit
     */
    public List<Row> executeLimit(LimitNode node, Function<PlanNode, List<Row>> childExecutor) {
        // 1. Ambil data dari anak
        List<Row> input = childExecutor.apply(node.child());
        
        // 2. Handle empty input
        if (input == null || input.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 3. Get limit value (int, not Integer, so always present)
        int limit = node.limit();
        
        // 4. Handle edge cases
        if (limit <= 0) {
            return Collections.emptyList(); // LIMIT 0 or negative = no rows
        }
        
        if (limit >= input.size()) {
            return new ArrayList<>(input); // LIMIT larger than data = return all
        }
        
        // 5. Return limited sublist
        return new ArrayList<>(input.subList(0, limit));
    }

    /**
     * [WEKA] - IMPLEMENTED
     * Menangani TCLNode: Perintah Transaksi (BEGIN, COMMIT, ROLLBACK).
     * 
     * TCL (Transaction Control Language) commands:
     * - BEGIN: Memulai transaksi baru
     * - COMMIT: Menyimpan perubahan transaksi
     * - ROLLBACK/ABORT: Membatalkan perubahan transaksi
     * 
     * @param node TCLNode yang berisi tipe perintah TCL
     * @param ccm Concurrency Control Manager
     * @param txId Transaction ID saat ini
     * @return List<Row> berisi status eksekusi
     */
    public List<Row> executeTCL(TCLNode node, IConcurrencyControlManager ccm, int txId) {
        String command = node.command().toUpperCase();
        
        Map<String, Object> resultData = new HashMap<>();
        
        switch (command) {
            case "BEGIN":
            case "BEGIN TRANSACTION":
                // BEGIN sudah di-handle di QueryProcessor.executeQuery()
                // Karena txId sudah di-generate sebelum node ini dieksekusi
                resultData.put("status", "Transaction already started");
                resultData.put("transaction_id", txId);
                System.out.println("[TCL] BEGIN TRANSACTION: txId=" + txId);
                break;
                
            case "COMMIT":
                // Commit transaksi
                ccm.endTransaction(txId, true);
                resultData.put("status", "Transaction committed");
                resultData.put("transaction_id", txId);
                System.out.println("[TCL] COMMIT: txId=" + txId);
                break;
                
            case "ROLLBACK":
            case "ABORT":
                // Rollback transaksi
                ccm.endTransaction(txId, false);
                resultData.put("status", "Transaction rolled back");
                resultData.put("transaction_id", txId);
                System.out.println("[TCL] ROLLBACK: txId=" + txId);
                break;
                
            default:
                throw new UnsupportedOperationException(
                    "Unsupported TCL command: " + command
                );
        }
        
        // Return status row
        return List.of(new Row(resultData));
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