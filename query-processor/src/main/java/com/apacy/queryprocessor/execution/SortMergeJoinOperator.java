package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.Row;

/**
 * Sort-Merge Join Operator implementation.
 * 
 * This operator performs an efficient join when:
 * 1. Input data is already sorted on the join column, OR
 * 2. Sorting cost is justified by the join efficiency
 * 
 * Algorithm:
 * - Sorts both input tables by the join column
 * - Merges them in a single pass using two pointers
 * - Time complexity: O(n log n + m log m) for sorting + O(n + m) for merging
 * 
 * Advantages over other join methods:
 * - More memory-efficient than Hash Join (no large hash table)
 * - Much faster than Nested Loop Join for large datasets
 * - Produces sorted output (useful for ORDER BY queries)
 */
public class SortMergeJoinOperator implements Operator {
    
    private final Operator leftChild;
    private final Operator rightChild;
    private final String joinColumn;
    
    // Materialized and sorted tables
    private List<Row> sortedLeftTable;
    private List<Row> sortedRightTable;
    
    // Merge state
    private int leftIndex = 0;
    private int rightIndex = 0;
    private List<Row> currentMatches = new ArrayList<>();
    private int matchIndex = 0;

    private static final int MEMORY_LIMIT_ROWS = 10_000;
    
    public SortMergeJoinOperator(Operator leftChild, Operator rightChild, String joinColumn) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinColumn = joinColumn;
    }

    @Override
    public void open() {
        leftChild.open();
        rightChild.open();
        
        System.out.println("[SortMergeJoinOperator] Building and sorting join inputs...");
        
        // Materialize left table
        sortedLeftTable = new ArrayList<>();
        Row row;
        while ((row = leftChild.next()) != null) {
            sortedLeftTable.add(row);
        }
        leftChild.close();
        
        // Materialize right table
        sortedRightTable = new ArrayList<>();
        while ((row = rightChild.next()) != null) {
            sortedRightTable.add(row);
        }
        rightChild.close();
        
        // Sort both tables by join column
        sortedLeftTable = SortStrategy.externalSort(sortedLeftTable, joinColumn, true, MEMORY_LIMIT_ROWS);
        sortedRightTable = SortStrategy.externalSort(sortedRightTable, joinColumn, true, MEMORY_LIMIT_ROWS);
        
        System.out.println("[SortMergeJoinOperator] Sort complete. Left: " + sortedLeftTable.size() + 
                         " rows, Right: " + sortedRightTable.size() + " rows");
        
        // Initialize merge pointers
        leftIndex = 0;
        rightIndex = 0;
        currentMatches.clear();
        matchIndex = 0;
    }

    @Override
    public Row next() {
        // If we have pending matches, emit them first
        if (matchIndex < currentMatches.size()) {
            return currentMatches.get(matchIndex++);
        }
        
        // Clear previous matches and find next set of matching rows
        currentMatches.clear();
        matchIndex = 0;
        
        // Merge phase: Two pointers approach
        while (leftIndex < sortedLeftTable.size() && rightIndex < sortedRightTable.size()) {
            Row leftRow = sortedLeftTable.get(leftIndex);
            Row rightRow = sortedRightTable.get(rightIndex);
            
            Object leftValue = getRowValue(leftRow, joinColumn);
            Object rightValue = getRowValue(rightRow, joinColumn);
            
            // Skip null values
            if (leftValue == null) {
                leftIndex++;
                continue;
            }
            if (rightValue == null) {
                rightIndex++;
                continue;
            }
            
            int comparison = compareValues(leftValue, rightValue);
            
            if (comparison < 0) {
                // Left value is smaller, advance left pointer
                leftIndex++;
            } else if (comparison > 0) {
                // Right value is smaller, advance right pointer
                rightIndex++;
            } else {
                // Values match! Handle duplicate keys by finding all matches
                int leftStart = leftIndex;
                int rightStart = rightIndex;
                
                // Find all left rows with the same key
                while (leftIndex < sortedLeftTable.size() && 
                       compareValues(SortStrategy.getRowValue(sortedLeftTable.get(leftIndex), joinColumn), leftValue) == 0) {
                    leftIndex++;
                }
                
                // Find all right rows with the same key
                while (rightIndex < sortedRightTable.size() && 
                       compareValues(SortStrategy.getRowValue(sortedRightTable.get(rightIndex), joinColumn), rightValue) == 0) {
                    rightIndex++;
                }
                
                // Create cartesian product of matching rows
                for (int i = leftStart; i < leftIndex; i++) {
                    for (int j = rightStart; j < rightIndex; j++) {
                        Row mergedRow = mergeJoinedRows(sortedLeftTable.get(i), sortedRightTable.get(j));
                        currentMatches.add(mergedRow);
                    }
                }
                
                // Return first match, rest will be returned in subsequent calls
                if (!currentMatches.isEmpty()) {
                    return currentMatches.get(matchIndex++);
                }
            }
        }
        
        // No more matches
        return null;
    }

    @Override
    public void close() {
        sortedLeftTable = null;
        sortedRightTable = null;
        currentMatches = null;
    }
    
    /**
     * Compare two values for sorting purposes.
     * Handles null values and different data types safely.
     */
    @SuppressWarnings("unchecked")
    private int compareValues(Object v1, Object v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return -1;
        if (v2 == null) return 1;
        
        // Handle Comparable types
        if (v1 instanceof Comparable && v2 instanceof Comparable) {
            try {
                return ((Comparable<Object>) v1).compareTo(v2);
            } catch (ClassCastException e) {
                // Fall back to string comparison if types don't match
                return v1.toString().compareTo(v2.toString());
            }
        }
        
        // Fall back to string comparison
        return v1.toString().compareTo(v2.toString());
    }
    
    /**
     * Merge two rows for a join operation, avoiding duplicate join columns.
     */
    private Row mergeJoinedRows(Row leftRow, Row rightRow) {
        Map<String, Object> mergedData = new HashMap<>(leftRow.data());
        
        for (Map.Entry<String, Object> entry : rightRow.data().entrySet()) {
            // Only add columns from right row that are not the join column
            if (!entry.getKey().equals(joinColumn)) {
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
