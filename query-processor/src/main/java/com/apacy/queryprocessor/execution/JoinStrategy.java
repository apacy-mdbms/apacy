package com.apacy.queryprocessor.execution;

import com.apacy.common.dto.Row;
import java.util.*;

/**
 * Implementation of various JOIN strategies.
 * Supports nested loop, hash, and sort-merge join algorithms for equi-joins.
 */
public class JoinStrategy {
    
    /**
     * Nested Loop Join implementation.
     * Compares each row in the left table with every row in the right table.
     * Time complexity: O(n * m) where n and m are table sizes.
     * 
     * @param leftTable The left table to join
     * @param rightTable The right table to join
     * @param joinColumn The column name to join on
     * @return List of joined rows
     */
    public static List<Row> nestedLoopJoin(List<Row> leftTable, List<Row> rightTable, String joinColumn) {
        if (leftTable == null || rightTable == null || joinColumn == null) {
            throw new IllegalArgumentException("Tables and join column cannot be null");
        }
        
        List<Row> result = new ArrayList<>();
        
        for (Row leftRow : leftTable) {
            Object leftValue = leftRow.get(joinColumn);
            
            // Skip rows with null join key
            if (leftValue == null) {
                continue;
            }
            
            for (Row rightRow : rightTable) {
                Object rightValue = rightRow.get(joinColumn);
                
                // Skip rows with null join key
                if (rightValue == null) {
                    continue;
                }
                
                // Check if join condition matches
                if (leftValue.equals(rightValue)) {
                    Row mergedRow = mergeRows(leftRow, rightRow);
                    result.add(mergedRow);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Hash Join implementation.
     * Builds a hash table from the right table, then probes with the left table.
     * Time complexity: O(n + m) average case.
     * 
     * @param leftTable The left table to join (probe side)
     * @param rightTable The right table to join (build side)
     * @param joinColumn The column name to join on
     * @return List of joined rows
     */
    public static List<Row> hashJoin(List<Row> leftTable, List<Row> rightTable, String joinColumn) {
        if (leftTable == null || rightTable == null || joinColumn == null) {
            throw new IllegalArgumentException("Tables and join column cannot be null");
        }
        
        List<Row> result = new ArrayList<>();
        
        // Build phase: Create hash table from right table
        Map<Object, List<Row>> hashTable = new HashMap<>();
        for (Row rightRow : rightTable) {
            Object rightValue = rightRow.get(joinColumn);
            
            // Skip rows with null join key
            if (rightValue == null) {
                continue;
            }
            
            hashTable.computeIfAbsent(rightValue, k -> new ArrayList<>()).add(rightRow);
        }
        
        // Probe phase: Lookup left table rows in hash table
        for (Row leftRow : leftTable) {
            Object leftValue = leftRow.get(joinColumn);
            
            // Skip rows with null join key
            if (leftValue == null) {
                continue;
            }
            
            // Find matching rows in hash table
            List<Row> matchingRightRows = hashTable.get(leftValue);
            if (matchingRightRows != null) {
                for (Row rightRow : matchingRightRows) {
                    Row mergedRow = mergeRows(leftRow, rightRow);
                    result.add(mergedRow);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Sort-Merge Join implementation.
     * Sorts both tables by join column, then merges them in a single pass.
     * Time complexity: O(n log n + m log m) for sorting, O(n + m) for merging.
     * 
     * @param leftTable The left table to join
     * @param rightTable The right table to join
     * @param joinColumn The column name to join on
     * @return List of joined rows
     */
    public static List<Row> sortMergeJoin(List<Row> leftTable, List<Row> rightTable, String joinColumn) {
        if (leftTable == null || rightTable == null || joinColumn == null) {
            throw new IllegalArgumentException("Tables and join column cannot be null");
        }
        
        List<Row> result = new ArrayList<>();
        
        // Sort both tables by join column
        List<Row> sortedLeft = SortStrategy.sort(leftTable, joinColumn, true);
        List<Row> sortedRight = SortStrategy.sort(rightTable, joinColumn, true);
        
        // Merge phase: Two pointers approach
        int leftIndex = 0;
        int rightIndex = 0;
        
        while (leftIndex < sortedLeft.size() && rightIndex < sortedRight.size()) {
            Row leftRow = sortedLeft.get(leftIndex);
            Row rightRow = sortedRight.get(rightIndex);
            
            Object leftValue = leftRow.get(joinColumn);
            Object rightValue = rightRow.get(joinColumn);
            
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
                // Values match, merge all matching rows
                // Handle duplicate keys by finding all matches
                int leftStart = leftIndex;
                int rightStart = rightIndex;
                
                // Find all left rows with the same key
                while (leftIndex < sortedLeft.size() && 
                       compareValues(sortedLeft.get(leftIndex).get(joinColumn), leftValue) == 0) {
                    leftIndex++;
                }
                
                // Find all right rows with the same key
                while (rightIndex < sortedRight.size() && 
                       compareValues(sortedRight.get(rightIndex).get(joinColumn), rightValue) == 0) {
                    rightIndex++;
                }
                
                // Create cartesian product of matching rows
                for (int i = leftStart; i < leftIndex; i++) {
                    for (int j = rightStart; j < rightIndex; j++) {
                        Row mergedRow = mergeRows(sortedLeft.get(i), sortedRight.get(j));
                        result.add(mergedRow);
                    }
                }
            }
        }
        
        return result;
    }

    /**
     * Cartesian Join implementation.
     * Combines every row in the left table with every row in the right table.
     * Time complexity: O(n * m) where n and m are table sizes.
     * 
     * @param leftTable The left table
     * @param rightTable The right table
     * @return List of joined rows (Cartesian product)
     */
    public static List<Row> cartesianJoin(List<Row> leftTable, List<Row> rightTable) {
        if (leftTable == null || rightTable == null) {
            throw new IllegalArgumentException("Tables cannot be null");
        }

        List<Row> result = new ArrayList<>();

        for (Row leftRow : leftTable) {
            for (Row rightRow : rightTable) {
                Row mergedRow = mergeRows(leftRow, rightRow);
                result.add(mergedRow);
            }
        }

        return result;
    }
    
    /**
     * Merge two rows into a single row by combining their data.
     * If both rows have the same column name, the left row's value takes precedence.
     * 
     * @param leftRow The left row
     * @param rightRow The right row
     * @return A new merged row
     */
    private static Row mergeRows(Row leftRow, Row rightRow) {
        Map<String, Object> mergedData = new HashMap<>(leftRow.data());
        
        // Add right row data, but don't overwrite existing columns
        for (Map.Entry<String, Object> entry : rightRow.data().entrySet()) {
            mergedData.putIfAbsent(entry.getKey(), entry.getValue());
        }
        
        return new Row(mergedData);
    }
    
    /**
     * Compare two values for sorting purposes.
     * Handles null values and different data types safely.
     * 
     * @param v1 First value
     * @param v2 Second value
     * @return Negative if v1 < v2, 0 if equal, positive if v1 > v2
     */
    @SuppressWarnings("unchecked")
    private static int compareValues(Object v1, Object v2) {
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
}