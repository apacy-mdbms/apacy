package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;

public class ProjectOperator implements Operator {
    private final Operator child;
    private final List<String> targetColumns;

    public ProjectOperator(Operator child, List<String> targetColumns) {
        this.child = child;
        this.targetColumns = targetColumns;
    }

    @Override
    public void open() {
        if (child == null) {
            throw new RuntimeException("ProjectOperator: child operator is null. PlanNode might be missing a child.");
        }
        child.open();
    }

    @Override
    public Row next() {
        Row row = child.next();
        if (row == null) return null;

        // Handle SELECT *
        if (targetColumns.size() == 1 && "*".equals(targetColumns.get(0))) {
            return row; 
        }

        Map<String, Object> newMap = new HashMap<>();
        for (String target : targetColumns) {
            if (row.data().containsKey(target)) {
                newMap.put(target, row.get(target));
            } 
            else {
                for (String key : row.data().keySet()) {
                    if (key.endsWith("." + target)) {
                        newMap.put(target, row.get(key));
                        break;
                    }
                }
            }
        }
        return new Row(newMap);
    }

    @Override
    public void close() {
        child.close();
    }
}
