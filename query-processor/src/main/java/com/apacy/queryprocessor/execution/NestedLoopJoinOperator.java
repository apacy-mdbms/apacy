package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;
import com.apacy.queryprocessor.evaluator.ConditionEvaluator;

/**
 * Block-Nested Loop Join Operator with Right Table Caching.
 * 
 * FIX I/O AMPLIFICATION:
 * - Caches the entire right table in memory on first open()
 * - Eliminates repeated rightChild.open() calls for each left row
 * - Reduces disk I/O from O(n*m) to O(n+m) where n = left rows, m = right rows
 */
public class NestedLoopJoinOperator implements Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final Object joinCondition;
    
    // Cached right table to avoid repeated disk reads
    private List<Row> rightTableCache;
    private boolean rightTableCached = false;
    
    // Iteration state
    private Row currentLeftRow;
    private int rightIndex;
    
    public NestedLoopJoinOperator(Operator leftChild, Operator rightChild, Object joinCondition) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
    }

    @Override
    public void open() {
        // Open left child
        leftChild.open();
        
        // Cache right table in memory (only once!)
        if (!rightTableCached) {
            cacheRightTable();
        }
        
        // Initialize iteration state
        currentLeftRow = leftChild.next();
        rightIndex = 0;
    }
    
    /**
     * Cache the entire right table in memory to avoid repeated disk reads.
     * This is called only once during the first open().
     */
    private void cacheRightTable() {
        rightTableCache = new ArrayList<>();
        rightChild.open();
        
        Row rightRow;
        while ((rightRow = rightChild.next()) != null) {
            rightTableCache.add(rightRow);
        }
        
        rightChild.close();
        rightTableCached = true;
        
        System.out.println("[NestedLoopJoin] Cached " + rightTableCache.size() + " rows from right table");
    }

    @Override
    public Row next() {
        while (currentLeftRow != null) {
            // Iterate through cached right table
            while (rightIndex < rightTableCache.size()) {
                Row rightRow = rightTableCache.get(rightIndex);
                rightIndex++;
                
                // Create merged row candidate
                Row mergedRow = mergeRows(currentLeftRow, rightRow);
                
                // Check join condition
                if (ConditionEvaluator.evaluate(mergedRow, joinCondition)) {
                    return mergedRow;
                }
            }
            
            // Right table exhausted for current left row, advance to next left row
            currentLeftRow = leftChild.next();
            rightIndex = 0; // Reset right index for new left row
        }
        
        return null; // Both sides exhausted
    }

    @Override
    public void close() {
        leftChild.close();
        rightChild.close();
    }

    private Row mergeRows(Row leftRow, Row rightRow) {
        Map<String, Object> mergedData = new HashMap<>(leftRow.data());
        // Right row data overrides collision? or preserve? 
        // Standard SQL: columns should be distinct or prefixed. 
        // Our previous JoinStrategy logic:
        // mergedData.putIfAbsent(entry.getKey(), entry.getValue()); (Left takes precedence)
        
        for (Map.Entry<String, Object> entry : rightRow.data().entrySet()) {
            mergedData.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return new Row(mergedData);
    }
}
