package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;

public class HashJoinOperator implements Operator {
    private final Operator probeChild; // Left side (Streamed)
    private final Operator buildChild; // Right side (Hashed)
    private final List<String> joinColumns;
    
    private Map<String, List<Row>> hashTable;
    private List<Row> currentMatches;
    private int matchIndex;
    
    private Row currentProbeRow;

    public HashJoinOperator(Operator probeChild, Operator buildChild, List<String> joinColumns) {
        this.probeChild = probeChild;
        this.buildChild = buildChild;
        this.joinColumns = joinColumns;
    }

    @Override
    public void open() {
        probeChild.open();
        buildChild.open();
        
        // Build Phase: Materialize Right Child into Hash Table
        hashTable = new HashMap<>();
        Row row;
        while ((row = buildChild.next()) != null) {
            String key = generateCompositeKey(row, joinColumns);
            if (key != null) {
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
        buildChild.close(); // Right side no longer needed
        
        // Prepare for Probe Phase
        currentProbeRow = probeChild.next();
        currentMatches = null;
        matchIndex = 0;
    }

    @Override
    public Row next() {
        while (currentProbeRow != null) {
            // If we haven't found matches for the current probe row yet
            if (currentMatches == null) {
                String key = generateCompositeKey(currentProbeRow, joinColumns);
                if (key != null && hashTable.containsKey(key)) {
                    currentMatches = hashTable.get(key);
                    matchIndex = 0;
                } else {
                    // No match, move to next probe row
                    currentProbeRow = probeChild.next();
                    continue;
                }
            }

            // If we have matches, emit them one by one
            if (matchIndex < currentMatches.size()) {
                Row matchedRight = currentMatches.get(matchIndex);
                matchIndex++;
                return mergeJoinedRows(currentProbeRow, matchedRight, joinColumns);
            } else {
                // Matches exhausted for this probe row
                currentMatches = null;
                currentProbeRow = probeChild.next();
            }
        }
        
        return null; // Probe exhausted
    }

    @Override
    public void close() {
        probeChild.close();
        hashTable = null;
        currentMatches = null;
    }

    private String generateCompositeKey(Row row, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        for (String col : columns) {
            Object val = row.get(col);
            if (val == null) return null; 
            sb.append(val).append("|");   
        }
        return sb.toString();
    }

    private Row mergeJoinedRows(Row leftRow, Row rightRow, List<String> joinColumns) {
        Map<String, Object> mergedData = new HashMap<>(leftRow.data());
        
        for (Map.Entry<String, Object> entry : rightRow.data().entrySet()) {
            if (!joinColumns.contains(entry.getKey())) {
                mergedData.put(entry.getKey(), entry.getValue());
            }
        }
        return new Row(mergedData);
    }
}
