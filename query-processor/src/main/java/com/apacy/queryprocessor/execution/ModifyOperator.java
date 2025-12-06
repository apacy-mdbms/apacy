package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataUpdate;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ForeignKeySchema;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.FactorNode;
import com.apacy.common.dto.ast.expression.LiteralFactor;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.where.BinaryConditionNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.ast.where.WhereConditionNode;
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
    
    DataRetrieval checkPK = new DataRetrieval(
        tableName,
        List.of(pkColumnName),
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
            List.of(fk.referenceColumn()),
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
            validateDelete(childRow, node.targetTable());

            frm.writeDataLog(String.valueOf(txId), "DELETE", node.targetTable(), childRow, null);

            Object predicate = buildIdentityAstFromRow(childRow);

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

            validateUpdatePrimaryKey(oldRow, newRow, node.targetTable()); 

            validateUpdateChild(oldRow, newRow, node.targetTable());

            validateUpdateParent(oldRow, newRow, node.targetTable());

            frm.writeDataLog(String.valueOf(txId), "UPDATE", node.targetTable(), oldRow, newRow);

            Object updatePredicate = buildIdentityAstFromRow(oldRow);
            DataUpdate du = new DataUpdate(node.targetTable(), newRow, updatePredicate);
            int updated = sm.updateBlock(du);
            affectedRows += updated;
        }
    }

    private void validateUpdatePrimaryKey(Row oldRow, Row newRow, String tableName) {
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

        Object oldValue = getColumnValue(oldRow, pkColumnName);
        Object newValue = getColumnValue(newRow, pkColumnName);

        if (newValue == null || newValue.equals(oldValue)) {
            return; 
        }

        if (newValue == null) {
            throw new RuntimeException("Primary Key Constraint Violation: Column '" + pkColumnName + "' cannot be NULL.");
        }

        Object filter = createEqualityCondition(pkColumnName, newValue);
        
        DataRetrieval checkPK = new DataRetrieval(
            tableName,
            List.of(pkColumnName),
            filter,
            true
        );

        List<Row> results = sm.readBlock(checkPK);

        if (!results.isEmpty()) {
            throw new RuntimeException("Primary Key Constraint Violation: Duplicate entry '" + newValue + 
                "' for key '" + pkColumnName + "' in table '" + tableName + "'.");
        }
    }
    private void validateUpdateChild(Row oldRow, Row newRow, String tableName) {
        Schema schema = sm.getSchema(tableName);
        if (schema.getForeignKeys() == null) return;

        for (ForeignKeySchema fk : schema.getForeignKeys()) {
            Object oldValue = getColumnValue(oldRow, fk.columnName());
            Object newValue = getColumnValue(newRow, fk.columnName());

            if (newValue != null && !newValue.equals(oldValue)) {
                Object filter = createEqualityCondition(fk.referenceColumn(), newValue);
                
                DataRetrieval checkParent = new DataRetrieval(
                    fk.referenceTable(),
                    List.of(fk.referenceColumn()),
                    filter,
                    true
                );

                if (sm.readBlock(checkParent).isEmpty()) {
                    throw new RuntimeException("Integrity Violation: Referenced key '" + newValue + 
                        "' not found in parent table '" + fk.referenceTable() + "'.");
                }
            }
        }
    }

    private void validateUpdateParent(Row oldRow, Row newRow, String tableName) {
        List<String> dependents = sm.getDependentTables(tableName);
        if (dependents == null || dependents.isEmpty()) return;

        for (String childTable : dependents) {
            Schema childSchema = sm.getSchema(childTable);
            if (childSchema == null) continue;

            for (ForeignKeySchema fk : childSchema.getForeignKeys()) {
                if (!fk.referenceTable().equalsIgnoreCase(tableName)) continue;

                Object oldKey = getColumnValue(oldRow, fk.referenceColumn());
                Object newKey = getColumnValue(newRow, fk.referenceColumn());

                if (oldKey != null && !oldKey.equals(newKey)) {
                    Object filter = createEqualityCondition(fk.columnName(), oldKey);
                    
                    DataRetrieval checkChildren = new DataRetrieval(
                        childTable,
                        List.of(fk.columnName()),
                        filter,
                        true
                    );
                    
                    List<Row> orphans = sm.readBlock(checkChildren);
                    if (!orphans.isEmpty()) {
                        throw new RuntimeException("Integrity Violation: Cannot update key '" + oldKey + 
                            "' in '" + tableName + "'. It is referenced by " + orphans.size() + 
                            " row(s) in '" + childTable + "'.");
                    }
                }
            }
        }
    }

    private void validateDelete(Row parentRow, String tableName) {
        List<String> dependents = sm.getDependentTables(tableName);
        if (dependents == null || dependents.isEmpty()) return;

        for (String childTableName : dependents) {
            Schema childSchema = sm.getSchema(childTableName);
            if (childSchema == null) continue;

            for (ForeignKeySchema fk : childSchema.getForeignKeys()) {
                if (!fk.referenceTable().equalsIgnoreCase(tableName)) continue;

                Object parentValue = getColumnValue(parentRow, fk.referenceColumn());
                if (parentValue == null) continue; 

                Object filter = createEqualityCondition(fk.columnName(), parentValue);

                DataRetrieval checkChild = new DataRetrieval(
                    childTableName,
                    List.of(fk.columnName()), 
                    filter,
                    true
                );

                List<Row> children = sm.readBlock(checkChild);

                if (!children.isEmpty()) {
                    if (fk.isCascading()) {
                        System.out.println("[Integrity] Cascading DELETE to table: " + childTableName);
                        
                        DataDeletion childDelete = new DataDeletion(childTableName, filter);
                        sm.deleteBlock(childDelete);
                        
                    } else {
                        throw new RuntimeException("Integrity Violation: Cannot delete row from '" + 
                            tableName + "'. It is still referenced by " + children.size() + 
                            " row(s) in table '" + childTableName + "'.");
                    }
                }
            }
        }
    }

    private Object getColumnValue(Row row, String columnName) {
        if (row.data().containsKey(columnName)) {
            return row.get(columnName);
        }
        
        String suffix = "." + columnName;
        for (String key : row.data().keySet()) {
            if (key.endsWith(suffix)) {
                return row.get(key);
            }
        }
        return null;
    }

    private Object buildIdentityAstFromRow(Row row) {
        Object currentCondition = null;

        for (Map.Entry<String, Object> e : row.data().entrySet()) {
            String colName = e.getKey();
            Object val = e.getValue();

            if (colName.contains(".")) {
                colName = colName.substring(colName.lastIndexOf('.') + 1);
            }

            Object comparison = createEqualityCondition(colName, val);

            if (currentCondition == null) {
                currentCondition = comparison;
            } else {
                currentCondition = new BinaryConditionNode(
                    (WhereConditionNode) currentCondition, 
                    "AND", 
                    (WhereConditionNode) comparison
                );
            }
        }
        return currentCondition;
    }

    private ComparisonConditionNode createEqualityCondition(String colName, Object val) {
        FactorNode leftFactor = new ColumnFactor(colName);
        TermNode leftTerm = new TermNode(leftFactor, null);
        ExpressionNode leftExpr = new ExpressionNode(leftTerm, null);

        FactorNode rightFactor = new LiteralFactor(val);
        TermNode rightTerm = new TermNode(rightFactor, null);
        ExpressionNode rightExpr = new ExpressionNode(rightTerm, null);

        return new ComparisonConditionNode(leftExpr, "=", rightExpr);
    }
}
