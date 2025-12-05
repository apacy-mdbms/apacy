package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ForeignKeySchema;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.plan.ModifyNode;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.queryprocessor.evaluator.ExpressionEvaluator;


public class ModifyOperator implements Operator {

    private final ModifyNode node;
    private final Operator child; // may be null for INSERT
    private final IStorageManager sm;
    private final IConcurrencyControlManager ccm;
    private final IFailureRecoveryManager frm;
    private final int txId;

    private boolean executed = false;
    private int affectedRows = 0;

    public ModifyOperator(ModifyNode node,
                          Operator child,
                          IStorageManager sm,
                          IConcurrencyControlManager ccm,
                          IFailureRecoveryManager frm,
                          int txId) {
        this.node = node;
        this.child = child;
        this.sm = sm;
        this.ccm = ccm;
        this.frm = frm;
        this.txId = txId;
    }

    @Override
    public void open() {
        if (child != null) {
            child.open();
        }
    }

    @Override
    public Row next() {
        if (executed) return null;

        try {
            String op = node.operation() != null ? node.operation().toUpperCase() : "";

            switch (op) {
                case "INSERT":
                    doInsert();
                    break;

                case "DELETE":
                    doDelete();
                    break;

                case "UPDATE":
                    doUpdate();
                    break;

                default:
                    throw new UnsupportedOperationException("ModifyOperator unsupported operation: " + op);
            }

            executed = true;
            Map<String, Object> result = Map.of("affected_rows", affectedRows);
            return new Row(result);

        } catch (RuntimeException e) {
            throw e;
        }
    }

    @Override
    public void close() {
        if (child != null) {
            child.close();
        }
    }

    private void doInsert() {
        Map<String, Object> dataMap = new HashMap<>();
        List<String> cols = node.targetColumns();
        List<Object> rawVals = node.values();

        List<Object> resolvedVals = new ArrayList<>();
        if (rawVals != null) {
            for (Object val : rawVals) {
                resolvedVals.add(ExpressionEvaluator.evaluate(val));
            }
        }

        if (cols != null && resolvedVals.size() == cols.size()) {
            for (int i = 0; i < cols.size(); i++) {
                dataMap.put(cols.get(i), resolvedVals.get(i));
            }
        } else {
            for (int i = 0; i < cols.size() && i < resolvedVals.size(); i++) {
                dataMap.put(cols.get(i), resolvedVals.get(i));
            }
        }

        Row newRow = new Row(dataMap);

        validatePrimaryKey(newRow, node.targetTable());

        validateInsert(newRow, node.targetTable());

        frm.writeDataLog(String.valueOf(txId), "INSERT", node.targetTable(), null, newRow);

        DataWrite dw = new DataWrite(node.targetTable(), newRow, null);
        affectedRows = sm.writeBlock(dw);
    }

    private void validatePrimaryKey(Row newRow, String tableName) {
    Schema schema = sm.getSchema(tableName);
    if (schema == null) return;

    String pkColumnName = null;
    String prefix = "pk_" + tableName + "_";

    for (IndexSchema idx : schema.indexes()) {
        if (idx.indexName().startsWith(prefix)) {
            pkColumnName = idx.columnName();
            break; 
        }
    }

    if (pkColumnName == null) return;

    Object pkValue = newRow.get(pkColumnName);

    if (pkValue == null) {
        throw new RuntimeException("Primary Key Violation: Column '" + pkColumnName + "' cannot be NULL.");
    }

    String filter = pkColumnName + "='" + pkValue + "'";
    
    DataRetrieval checkPK = new com.apacy.common.dto.DataRetrieval(
        tableName,
        java.util.List.of(pkColumnName),
        filter,
        true
    );

    List<Row> results = sm.readBlock(checkPK);

    if (!results.isEmpty()) {
        throw new RuntimeException("Primary Key Violation: Duplicate entry '" + pkValue + "' for key '" + pkColumnName + "'.");
    }
}

    private void validateInsert(Row newRow, String tableName) {
    Schema schema = sm.getSchema(tableName);

    System.out.println(schema.getForeignKeys());
    
    if (schema == null || schema.getForeignKeys() == null) return;

    for (ForeignKeySchema fk : schema.getForeignKeys()) {
        
        Object childValue = newRow.get(fk.columnName());

        if (childValue == null) continue;

        String filter = fk.referenceColumn() + "='" + childValue + "'";
        
        DataRetrieval checkParent = new DataRetrieval(
            fk.referenceTable(),
            java.util.List.of(fk.referenceColumn()),
            filter,
            true
        );

        List<Row> results = sm.readBlock(checkParent);

        if (results.isEmpty()) {
            throw new RuntimeException("Integrity Constraint Violation: The value '" + childValue + 
                "' for column '" + fk.columnName() + "' does not exist in referenced table '" + 
                fk.referenceTable() + "'.");
        }
    }
}

    private void doDelete() {
        if (child == null) {
            return;
        }

        Row childRow;
        while ((childRow = child.next()) != null) {
            frm.writeDataLog(String.valueOf(txId), "DELETE", node.targetTable(), childRow, null);

            Object predicate = buildPredicateFromRow(childRow);

            DataDeletion dd = new DataDeletion(node.targetTable(), predicate);
            int deleted = sm.deleteBlock(dd);
            affectedRows += deleted;
        }
    }

    private void doUpdate() {
        if (child == null) {
            return;
        }

        List<Object> resolvedVals = new ArrayList<>();
        if (node.values() != null) {
            for (Object val : node.values()) {
                resolvedVals.add(ExpressionEvaluator.evaluate(val));
            }
        }

        Row oldRow;
        while ((oldRow = child.next()) != null) {
            Map<String, Object> mergedData = new HashMap<>(oldRow.data());

            List<String> targetCols = node.targetColumns();
            if (targetCols != null && resolvedVals.size() == targetCols.size()) {
                for (int i = 0; i < targetCols.size(); i++) {
                    mergedData.put(targetCols.get(i), resolvedVals.get(i));
                }
            }

            Row newRow = new Row(mergedData);

            frm.writeDataLog(String.valueOf(txId), "UPDATE", node.targetTable(), oldRow, newRow);

            Object deletePredicate = buildPredicateFromRow(oldRow);
            sm.deleteBlock(new DataDeletion(node.targetTable(), deletePredicate));

            DataWrite dw = new DataWrite(node.targetTable(), newRow, null);
            int written = sm.writeBlock(dw);
            affectedRows += written;
        }
    }

    private String buildPredicateFromRow(Row row) {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (Map.Entry<String, Object> e : row.data().entrySet()) {
            if (!first) {
                sb.append(" AND ");
            }
            first = false;

            String col = e.getKey();
            Object val = e.getValue();

            if (val == null) {
                sb.append(col).append(" IS NULL");
            } else if (val instanceof String) {
                String escaped = escapeSingleQuotes((String) val);
                sb.append(col).append("='").append(escaped).append("'");
            } else {
                sb.append(col).append("=").append(val.toString());
            }
        }

        return sb.toString();
    }

    private String escapeSingleQuotes(String s) {
        if (s == null) return null;
        return s.replace("'", "''");
    }
}
