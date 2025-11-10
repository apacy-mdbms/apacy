package com.apacy.queryprocessor.execution;

import java.util.ArrayList;
import java.util.List;

import com.apacy.common.dto.Row;

/**
 * Implementation of various sorting strategies for ORDER BY clauses.
 * TODO: Implement efficient sorting algorithms including external sort for large datasets
 */
public class SortStrategy {
    
    /**
     * Sort rows by a specific column.
     * Sort Algorithm: Quicksort
     */
    public static List<Row> sort(List<Row> rows, String columnName, boolean ascending) {
        if (rows == null || rows.size() <= 1) {
            return rows;
        }

        Comparator customComparator = createColumnComparator(columnName, ascending);

        List<Row> listToSort = new ArrayList<>(rows);

        quicksort(listToSort, 0, listToSort.size() - 1, customComparator);

        return listToSort;
    }
    
    /**
     * Sort by multiple columns with different sort orders.
     * Sort Algorithm: Quicksort
     */
    public static List<Row> sortMultiple(List<Row> rows, String[] columnNames, boolean[] ascending) {
        if (rows == null || rows.size() <= 1 || columnNames == null || columnNames.length == 0) {
            return rows;
        }
        if (columnNames.length != ascending.length) {
            throw new IllegalArgumentException("Jumlah kolom dan flag 'ascending' harus sama.");
        }

        Comparator multiComparator = (r1, r2) -> {
            for (int i = 0; i < columnNames.length; i++) {
                int result = compareValues(r1.get(columnNames[i]), r2.get(columnNames[i]));
                if (result != 0) {
                    return ascending[i] ? result : -result;
                }
            }
            return 0;
        };

        List<Row> listToSort = new ArrayList<>(rows);
        
        quicksort(listToSort, 0, listToSort.size() - 1, multiComparator);

        return listToSort;
    }

    // Quicksort Algorithm
    private static void quicksort(List<Row> list, int low, int high, Comparator comparator) {
        if (low < high) {
            int pivotIndex = partition(list, low, high, comparator);

            quicksort(list, low, pivotIndex - 1, comparator);
            quicksort(list, pivotIndex + 1, high, comparator);
        }
    }

    private static int partition(List<Row> list, int low, int high, Comparator comparator) {
        Row pivot = list.get(high);
        
        int i = (low - 1); 

        for (int j = low; j < high; j++) {
            if (comparator.compare(list.get(j), pivot) <= 0) {
                i++;
                swap(list, i, j);
            }
        }

        swap(list, i + 1, high);
        
        return (i + 1);
    }

    private static void swap(List<Row> list, int i, int j) {
        Row temp = list.get(i);
        list.set(i, list.get(j));
        list.set(j, temp);
    }
    
    /**
     * External sort for large datasets that don't fit in memory.
     * TODO: Implement external merge sort with temporary file management
     */
    public static List<Row> externalSort(List<Row> rows, String columnName, boolean ascending, 
                                        int memoryLimit) {
        // TODO: Implement external sorting algorithm
        throw new UnsupportedOperationException("externalSort not implemented yet");
    }

    // Helper buat perbandingan row
    private interface Comparator {
        int compare(Row r1, Row r2);
    }

    private static Comparator createColumnComparator(String columnName, boolean ascending) {
        Comparator comp = (r1, r2) -> compareValues(r1.get(columnName), r2.get(columnName));
        if (ascending) return comp;
        else return (r1, r2) -> -comp.compare(r1, r2); 
    }

    // Note: ga bisa buat perbandingan beda tipe (Integer vs Double)
    private static int compareValues(Object v1, Object v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return 1;
        if (v2 == null) return -1;
        if (v1 instanceof Comparable && v2 instanceof Comparable) {
            if (v1.getClass().equals(v2.getClass())) {
                try {
                    return ((Comparable) v1).compareTo(v2);
                } catch (Exception e) { /* fallback */ }
            }
        }
        return v1.toString().compareTo(v2.toString());
    }
}