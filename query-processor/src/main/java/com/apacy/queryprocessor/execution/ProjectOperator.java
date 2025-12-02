package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;
import com.apacy.common.dto.plan.ProjectNode;

public class ProjectOperator implements Operator {
    private final Operator child;
    private final List<String> targetColumns;

    public ProjectOperator(Operator child, List<String> targetColumns) {
        this.child = child;
        this.targetColumns = targetColumns;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Row next() {
        Row row = child.next();
        if (row == null) {
            return null;
        }

        // Handle "SELECT *"
        if (targetColumns.size() == 1 && "*".equals(targetColumns.get(0))) {
            return row;
        }

        Map<String, Object> newMap = new HashMap<>();
        for (String col : targetColumns) {
            if (row.data().containsKey(col)) {
                newMap.put(col, row.get(col));
            }
        }
        return new Row(newMap);
    }

    @Override
    public void close() {
        child.close();
    }
}
