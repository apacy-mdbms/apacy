package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IStorageManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class MockStorageManager implements IStorageManager {

    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        
        List<Row> dummy = new ArrayList<>();

        Row row1 = new Row(Map.of(
            "id", 1,
            "name", "Naufarrel",
            "salary", 20000
        ));

        Row row2 = new Row(Map.of(
            "id", 2,
            "name", "Weka",
            "salary", 30000
        ));

        Row row3 = new Row(Map.of(
            "id", 3,
            "name", "Kinan",
            "salary", 40000
        ));

        Row row4 = new Row(Map.of(
            "id", 4,
            "name", "Farrel",
            "salary", 50000
        ));

        Row row5 = new Row(Map.of(
            "id", 5,
            "name", "Bayu",
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
        throw new UnsupportedOperationException("writeBlock not implemented yet");
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        throw new UnsupportedOperationException("deleteBlock not implemented yet");
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        throw new UnsupportedOperationException("setIndex not implemented yet");
    }

    @Override
    public Map<String, Statistic> getAllStats() {
        System.out.println("[MOCK-SM] getAllStats() dipanggil. Mengembalikan statistik palsu.");

        Map<String, Statistic> mockStatsMap = new HashMap<>();

        Statistic userStats = new Statistic(
            3,  // nr (jumlah tuple)
            1,  // br (jumlah blok)
            50, // lr (ukuran tuple)
            10, // fr (blocking factor)
            Map.of("id", 3, "salary", 2) // V (nilai unik)
        );

        Statistic deptStats = new Statistic(
            2, 1, 30, 10, Map.of("dept_id", 2)
        );

        mockStatsMap.put("users", userStats);
        mockStatsMap.put("departments", deptStats);

        return mockStatsMap;
    }
}