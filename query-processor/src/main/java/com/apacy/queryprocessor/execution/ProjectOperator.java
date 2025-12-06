package com.apacy.queryprocessor.execution;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
            throw new RuntimeException("ProjectOperator: child operator is null.");
        }
        child.open();
    }

    @Override
    public Row next() {
        Row row = child.next();
        if (row == null) return null;

        if (targetColumns.size() == 1 && "*".equals(targetColumns.get(0))) {
            return row; 
        }

        Map<String, Object> newMap = new LinkedHashMap<>();
        
        for (String target : targetColumns) {
            if ("*".equals(target)) {
                newMap.putAll(row.data());
                continue;
            }

            if (row.data().containsKey(target)) {
                newMap.put(target, row.get(target));
                continue;
            } 
            
            String foundKey = null;
            for (String key : row.data().keySet()) {
                if (key.endsWith("." + target)) {
                    if (foundKey != null) {
                        throw new RuntimeException("Ambiguous column in projection: '" + target + 
                            "' matches both '" + foundKey + "' and '" + key + "'");
                    }
                    foundKey = key;
                }
            }

            if (foundKey != null) {
                newMap.put(target, row.get(foundKey));
            } else {
                throw new RuntimeException("Execution Error: Column '" + target + "' not found in result set. Available columns: " + row.data().keySet());
            }
        }
        return new Row(newMap);
    }

    @Override
    public void close() {
        child.close();
    }
}