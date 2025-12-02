package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.ModifyNode;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IStorageManager;

public class ModifyOperator implements Operator {
    private final ModifyNode node;
    private final IStorageManager sm;
    private boolean executed = false;

    public ModifyOperator(ModifyNode node, IStorageManager sm) {
        this.node = node;
        this.sm = sm;
    }

    @Override
    public void open() {
        // Nothing to open
    }

    @Override
    public Row next() {
        if (executed) {
            return null;
        }

        int affectedRows = 0;
        String operation = node.operation().toUpperCase();

        if ("INSERT".equals(operation)) {
            Map<String, Object> dataMap = new HashMap<>();
            List<String> cols = node.targetColumns();
            List<Object> vals = node.values();

            if (cols == null || cols.isEmpty()) {
                // Fetch schema from Storage Manager if columns are not specified
                try {
                    var schema = sm.getSchema(node.targetTable());
                    if (schema != null) {
                        cols = schema.columns().stream()
                                .map(com.apacy.common.dto.Column::name)
                                .toList();
                    }
                } catch (Exception e) {
                    // Ignore or handle error
                    System.err.println("Warning: Could not fetch schema for table " + node.targetTable());
                }
            }

            if (cols != null && vals != null && cols.size() == vals.size()) {
                for (int i = 0; i < cols.size(); i++) {
                    dataMap.put(cols.get(i), vals.get(i));
                }
            }
            DataWrite dw = new DataWrite(node.targetTable(), new Row(dataMap), null);
            affectedRows = sm.writeBlock(dw);
            
            if (affectedRows == 0) {
                throw new RuntimeException("INSERT failed: Zero rows affected. Possible Duplicate Primary Key or Constraint Violation.");
            }

        } else if ("DELETE".equals(operation)) {
            Object filterCondition = null;
            if (!node.getChildren().isEmpty() && node.getChildren().get(0) instanceof FilterNode fn) {
                filterCondition = fn.predicate();
            }
            DataDeletion dd = new DataDeletion(node.targetTable(), filterCondition);
            affectedRows = sm.deleteBlock(dd);

        } else if ("UPDATE".equals(operation)) {
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

            System.out.println("[DEBUG] ModifyOperator UPDATE: Table=" + node.targetTable() + ", Data=" + dataMap);

            DataWrite dw = new DataWrite(node.targetTable(), new Row(dataMap), filterCondition);
            affectedRows = sm.writeBlock(dw);
        }

        executed = true;
        Map<String, Object> resultData = new HashMap<>();
        resultData.put("affected_rows", affectedRows);
        return new Row(resultData);
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
