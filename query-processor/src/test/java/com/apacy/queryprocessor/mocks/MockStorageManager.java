package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.enums.IndexType;
import com.apacy.common.interfaces.IStorageManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class MockStorageManager implements IStorageManager {

    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        System.out.println("[MOCK-SM] readBlock dipanggil untuk tabel: " + dataRetrieval.tableName());
        System.out.println("[MOCK-SM] Kolom yang diminta: " + dataRetrieval.columns());
        System.out.println("[MOCK-SM] Filter: " + dataRetrieval.filterCondition());
        System.out.println("[MOCK-SM] Use Index: " + dataRetrieval.useIndex());
        
        List<Row> dummy = new ArrayList<>();

        Row row1 = new Row(Map.of(
            "id", 1,
            "name", "Naufarrel",
            "email", "naufarrel@example.com",
            "salary", 20000
        ));

        Row row2 = new Row(Map.of(
            "id", 2,
            "name", "Weka",
            "email", "weka@example.com", 
            "salary", 30000
        ));

        Row row3 = new Row(Map.of(
            "id", 3,
            "name", "Kinan",
            "email", "kinan@example.com",
            "salary", 40000
        ));

        Row row4 = new Row(Map.of(
            "id", 4,
            "name", "Farrel",
            "email", "farrel@example.com",
            "salary", 50000
        ));

        Row row5 = new Row(Map.of(
            "id", 5,
            "name", "Bayu",
            "email", "bayu@example.com",
            "salary", 60000
        ));
        
        dummy.add(row1);
        dummy.add(row2);
        dummy.add(row3);
        dummy.add(row4);
        dummy.add(row5);

        return dummy;
    }

    @Override
    public int writeBlock(DataWrite dataWrite) {
        System.out.println("[MOCK-SM] writeBlock dipanggil untuk tabel: " + dataWrite.tableName());
        System.out.println("[MOCK-SM] Data baru: " + dataWrite.newData().data());
        System.out.println("[MOCK-SM] Filter: " + dataWrite.filterCondition());
        
        // Simulasikan berhasil menulis 1 row
        return 1;
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        System.out.println("[MOCK-SM] deleteBlock dipanggil untuk tabel: " + dataDeletion.tableName());
        System.out.println("[MOCK-SM] Filter: " + dataDeletion.filterCondition());
        
        // Simulasikan berhasil menghapus 1 row
        return 1;
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        System.out.println("[MOCK-SM] setIndex dipanggil:");
        System.out.println("  - tabel: " + table);
        System.out.println("  - kolom: " + column);
        System.out.println("  - tipe index: " + indexType);
    }

    @Override
    public Map<String, Statistic> getAllStats() {
        System.out.println("[MOCK-SM] getAllStats() dipanggil. Mengembalikan statistik palsu.");

        Map<String, Statistic> mockStatsMap = new HashMap<>();

        // Update sesuai dengan Statistic record yang baru (dengan indexedColumn)
        Statistic userStats = new Statistic(
            5,  // nr (jumlah tuple) - update sesuai data mock
            1,  // br (jumlah blok)
            120, // lr (ukuran tuple) - lebih besar karena ada email
            34, // fr (blocking factor)
            Map.of("id", 5, "name", 5, "email", 5, "salary", 5), // V (nilai unik untuk setiap kolom)
            Map.of("id", IndexType.Hash, "salary", IndexType.BPlusTree) // indexedColumn - kolom yang diindeks
        );

        Statistic deptStats = new Statistic(
            2, // nr
            1, // br
            80, // lr
            50, // fr
            Map.of("dept_id", 2, "dept_name", 2), // V
            Map.of("dept_id", IndexType.Hash) // indexedColumn
        );

        mockStatsMap.put("users", userStats);
        mockStatsMap.put("departments", deptStats);

        return mockStatsMap;
    }
}