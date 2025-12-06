package com.apacy.queryprocessor.execution;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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

        Comparator<Row> customComparator = createColumnComparator(columnName, ascending);

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

        Comparator<Row> multiComparator = (r1, r2) -> {
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
    private static void quicksort(List<Row> list, int low, int high, Comparator<Row> comparator) {
        if (low < high) {
            int pivotIndex = partition(list, low, high, comparator);

            quicksort(list, low, pivotIndex - 1, comparator);
            quicksort(list, pivotIndex + 1, high, comparator);
        }
    }

    private static int partition(List<Row> list, int low, int high, Comparator<Row> comparator) {
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
     * External Merge Sort implementation.
     */
    public static List<Row> externalSort(List<Row> rows, String columnName, boolean ascending, int memoryLimit) {
        if (rows == null || rows.isEmpty()) return new ArrayList<>();
        
        // 1. Jika data muat di memori, gunakan in-memory sort biasa (lebih cepat)
        if (rows.size() <= memoryLimit) {
            return sort(rows, columnName, ascending);
        }

        List<File> tempFiles = new ArrayList<>();
        Comparator<Row> comparator = createColumnComparator(columnName, ascending);

        try {
            // 2. PHASE 1: SPLIT, SORT, & SPILL
            // Pecah data menjadi chunk kecil, sort di RAM, tulis ke disk
            int chunkCount = (int) Math.ceil((double) rows.size() / memoryLimit);
            
            for (int i = 0; i < chunkCount; i++) {
                int start = i * memoryLimit;
                int end = Math.min(start + memoryLimit, rows.size());
                
                List<Row> chunk = new ArrayList<>(rows.subList(start, end));
                chunk.sort(comparator); // Sort potongan kecil di memori
                
                tempFiles.add(saveChunkToTempFile(chunk));
            }

            // 3. PHASE 2: K-WAY MERGE
            // Gabungkan potongan-potongan tersebut secara streaming
            return mergeSortedFiles(tempFiles, comparator);

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("External Sort failed: " + e.getMessage(), e);
        } finally {
            // Cleanup: Hapus file temporary setelah selesai
            for (File f : tempFiles) {
                if (f.exists()) f.delete();
            }
        }
    }

    // --- Helper Methods untuk External Sort ---

    private static File saveChunkToTempFile(List<Row> chunk) throws IOException {
        File tempFile = File.createTempFile("apacy_sort_", ".tmp");
        tempFile.deleteOnExit(); 

        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                new java.io.BufferedOutputStream(new java.io.FileOutputStream(tempFile)))) {
            // Tulis jumlah baris di header file
            oos.writeInt(chunk.size());
            for (Row row : chunk) {
                oos.writeObject(row);
            }
        }
        return tempFile;
    }

    private static List<Row> mergeSortedFiles(List<File> files, Comparator<Row> comparator) 
            throws IOException, ClassNotFoundException {
        List<Row> result = new ArrayList<>();
        
        // Min-Heap untuk mengambil elemen terkecil dari N file secara efisien
        java.util.PriorityQueue<FileBatch> pq = new java.util.PriorityQueue<>(
            (fb1, fb2) -> comparator.compare(fb1.peek(), fb2.peek())
        );
        
        List<java.io.ObjectInputStream> openStreams = new ArrayList<>();

        try {
            // Buka semua file dan masukkan elemen pertamanya ke antrian
            for (File file : files) {
                java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                    new java.io.BufferedInputStream(new java.io.FileInputStream(file)));
                openStreams.add(ois);
                
                int rowCount = ois.readInt();
                if (rowCount > 0) {
                    pq.add(new FileBatch(ois, rowCount));
                } else {
                    ois.close();
                }
            }

            while (!pq.isEmpty()) {
                FileBatch batch = pq.poll();
                result.add(batch.pop());

                if (batch.hasNext()) {
                    batch.reloadNext();
                    pq.add(batch);
                } else {
                    batch.close();
                }
            }
        } finally {
            for (java.io.ObjectInputStream ois : openStreams) {
                try { ois.close(); } catch (IOException ignored) {}
            }
        }

        return result;
    }

    private static class FileBatch {
        private final java.io.ObjectInputStream ois;
        private int remaining;
        private Row currentRow;

        FileBatch(java.io.ObjectInputStream ois, int total) throws IOException, ClassNotFoundException {
            this.ois = ois;
            this.remaining = total;
            reloadNext();
        }

        void reloadNext() throws IOException, ClassNotFoundException {
            if (remaining > 0) {
                this.currentRow = (Row) ois.readObject();
                remaining--;
            } else {
                this.currentRow = null;
            }
        }

        Row peek() { return currentRow; }

        Row pop() {
            Row r = currentRow;
            currentRow = null;
            return r;
        }

        boolean hasNext() { return currentRow != null || remaining > 0; }
        
        void close() throws IOException { ois.close(); }
    }

    private static Comparator<Row> createColumnComparator(String columnName, boolean ascending) {
        Comparator<Row> comp = (r1, r2) -> compareValues(
            getRowValue(r1, columnName), 
            getRowValue(r2, columnName)
        );
        
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
                    return ((Comparable<Object>) v1).compareTo(v2);
                } catch (Exception e) { /* fallback */ }
            }
        }
        return v1.toString().compareTo(v2.toString());
    }

    public static Object getRowValue(Row row, String columnName) {
        if (row.data().containsKey(columnName)) {
            return row.get(columnName);
        }
        
        String suffix = "." + columnName;
        if (columnName.contains(".")) {
            suffix = "." + columnName.substring(columnName.lastIndexOf('.') + 1);
        }
        
        for (String key : row.data().keySet()) {
            if (key.endsWith(suffix) || key.equalsIgnoreCase(columnName)) {
                return row.get(key);
            }
        }
        return null;
    }
}