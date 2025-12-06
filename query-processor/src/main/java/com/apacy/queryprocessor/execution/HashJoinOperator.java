package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;

public class HashJoinOperator implements Operator {
    // Safety thresholds
    private static final int DEFAULT_MAX_BUILD_ROWS = 100_000; // Default: 100K rows
    private static final int DEFAULT_MAX_HASH_TABLE_SIZE_MB = 512; // Default: 512MB
    
    private final Operator probeChild; // Left side (Streamed)
    private final Operator buildChild; // Right side (Hashed)
    private final List<String> joinColumns;
    private final int maxBuildRows;
    private final long maxHashTableSizeBytes;
    
    private Map<String, List<Row>> hashTable;
    private List<Row> currentMatches;
    private int matchIndex;
    
    private Row currentProbeRow;
    private boolean useFallbackMode = false;
    private NestedLoopJoinOperator fallbackOperator;

    public HashJoinOperator(Operator probeChild, Operator buildChild, List<String> joinColumns) {
        this(probeChild, buildChild, joinColumns, DEFAULT_MAX_BUILD_ROWS, DEFAULT_MAX_HASH_TABLE_SIZE_MB);
    }
    
    public HashJoinOperator(Operator probeChild, Operator buildChild, List<String> joinColumns, 
                           int maxBuildRows, int maxHashTableSizeMB) {
        this.probeChild = probeChild;
        this.buildChild = buildChild;
        this.joinColumns = joinColumns;
        this.maxBuildRows = maxBuildRows;
        this.maxHashTableSizeBytes = maxHashTableSizeMB * 1024L * 1024L;
    }

    @Override
    public void open() {
        probeChild.open();
        buildChild.open();
        
        // Build Phase: Materialize Right Child into Hash Table with safety checks
        hashTable = new HashMap<>();
        Row row;
        int rowCount = 0;
        long estimatedMemoryUsage = 0;
        
        try {
            while ((row = buildChild.next()) != null) {
                rowCount++;
                
                // Check row count limit
                if (rowCount > maxBuildRows) {
                    System.err.println("[HashJoinOperator] WARNING: Build side exceeds " + maxBuildRows + 
                                     " rows. Falling back to Nested Loop Join.");
                    triggerFallbackMode();
                    return;
                }
                
                // Estimate memory usage (rough approximation)
                estimatedMemoryUsage += estimateRowSize(row);
                if (estimatedMemoryUsage > maxHashTableSizeBytes) {
                    System.err.println("[HashJoinOperator] WARNING: Estimated memory usage (" + 
                                     (estimatedMemoryUsage / 1024 / 1024) + " MB) exceeds limit (" + 
                                     (maxHashTableSizeBytes / 1024 / 1024) + " MB). Falling back to Nested Loop Join.");
                    triggerFallbackMode();
                    return;
                }
                
                String key = generateCompositeKey(row, joinColumns);
                if (key != null) {
                    hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                }
            }
            
            buildChild.close(); // Right side no longer needed
            
            System.out.println("[HashJoinOperator] Successfully built hash table with " + rowCount + 
                             " rows (~" + (estimatedMemoryUsage / 1024 / 1024) + " MB)");
            
            // Prepare for Probe Phase
            currentProbeRow = probeChild.next();
            currentMatches = null;
            matchIndex = 0;
            
        } catch (Exception e) {
            System.err.println("[HashJoinOperator] ERROR during build phase: " + e.getMessage());
            triggerFallbackMode();
        }
    }

    @Override
    public Row next() {
        // If fallback mode is active, delegate to Nested Loop Join
        if (useFallbackMode) {
            return fallbackOperator.next();
        }
        
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
        if (useFallbackMode && fallbackOperator != null) {
            fallbackOperator.close();
        } else {
            probeChild.close();
        }
        hashTable = null;
        currentMatches = null;
    }
    
    /**
     * Triggers fallback to Nested Loop Join when hash table becomes too large.
     * This ensures the system doesn't run out of memory.
     */
    private void triggerFallbackMode() {
        useFallbackMode = true;
        
        // Clean up hash table to free memory
        if (hashTable != null) {
            hashTable.clear();
            hashTable = null;
        }
        
        // Close and reopen children to reset their state
        buildChild.close();
        probeChild.close();
        
        // Create join condition based on join columns
        Object joinCondition = createJoinCondition(joinColumns);
        
        // Initialize fallback operator
        fallbackOperator = new NestedLoopJoinOperator(probeChild, buildChild, joinCondition);
        fallbackOperator.open();
    }
    
    /**
     * Creates a join condition for Nested Loop Join based on join columns.
     */
    private Object createJoinCondition(List<String> columns) {
        // Create an equality condition for the join columns
        // Format: "left.col1 = right.col1 AND left.col2 = right.col2 ..."
        StringBuilder condition = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                condition.append(" AND ");
            }
            String col = columns.get(i);
            condition.append(col).append(" = ").append(col);
        }
        return condition.toString();
    }
    
    /**
     * Estimates the memory size of a Row in bytes.
     * This is a rough approximation based on object overhead and data.
     */
    private long estimateRowSize(Row row) {
        long size = 48; // Base object overhead (approximate)
        
        for (Map.Entry<String, Object> entry : row.data().entrySet()) {
            // Key (String)
            size += 40 + (entry.getKey().length() * 2); // String overhead + chars
            
            // Value
            Object value = entry.getValue();
            if (value == null) {
                size += 4;
            } else if (value instanceof String string) {
                size += 40 + (string.length() * 2);
            } else if (value instanceof Integer) {
                size += 16;
            } else if (value instanceof Long) {
                size += 16;
            } else if (value instanceof Double) {
                size += 16;
            } else if (value instanceof Boolean) {
                size += 16;
            } else {
                size += 32; // Generic object overhead
            }
        }
        
        return size;
    }

    private String generateCompositeKey(Row row, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        for (String col : columns) {
            Object val = getRowValue(row, col);
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

    private Object getRowValue(Row row, String columnName) {
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
}
