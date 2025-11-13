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
        String tableName = dataRetrieval.tableName();
        
        // Route to appropriate table data
        switch (tableName) {
            case "employees":
                return getEmployeeData();
            case "departments":
                return getDepartmentData();
            default:
                // BACKWARD COMPATIBILITY: Return original employee data for any unknown table
                // This matches the old behavior where it always returned employee data
                return getEmployeeDataLegacy();
        }
    }
    
    /**
     * Legacy employee data (without dept_id) for backward compatibility.
     * This is the ORIGINAL format that was always returned before multi-table support.
     */
    private List<Row> getEmployeeDataLegacy() {
        List<Row> employees = new ArrayList<>();
        
        employees.add(new Row(Map.of(
            "id", 1,
            "name", "Naufarrel",
            "salary", 20000
        )));
        
        employees.add(new Row(Map.of(
            "id", 2,
            "name", "Weka",
            "salary", 30000
        )));
        
        employees.add(new Row(Map.of(
            "id", 3,
            "name", "Kinan",
            "salary", 40000
        )));
        
        employees.add(new Row(Map.of(
            "id", 4,
            "name", "Farrel",
            "salary", 50000
        )));
        
        employees.add(new Row(Map.of(
            "id", 5,
            "name", "Bayu",
            "salary", 60000
        )));
        
        return employees;
    }
    
    /**
     * New employee data with dept_id for join operations.
     * Explicitly request with tableName = "employees"
     */
    private List<Row> getEmployeeData() {
        List<Row> employees = new ArrayList<>();
        
        employees.add(new Row(Map.of(
            "id", 1,
            "name", "Naufarrel",
            "salary", 20000,
            "dept_id", 1
        )));
        
        employees.add(new Row(Map.of(
            "id", 2,
            "name", "Weka",
            "salary", 30000,
            "dept_id", 2
        )));
        
        employees.add(new Row(Map.of(
            "id", 3,
            "name", "Kinan",
            "salary", 40000,
            "dept_id", 1
        )));
        
        employees.add(new Row(Map.of(
            "id", 4,
            "name", "Farrel",
            "salary", 50000,
            "dept_id", 3
        )));
        
        employees.add(new Row(Map.of(
            "id", 5,
            "name", "Bayu",
            "salary", 60000,
            "dept_id", 2
        )));
        
        return employees;
    }
    
    private List<Row> getDepartmentData() {
        List<Row> departments = new ArrayList<>();
        
        departments.add(new Row(Map.of(
            "dept_id", 1,
            "dept_name", "Engineering",
            "location", "Building A"
        )));
        
        departments.add(new Row(Map.of(
            "dept_id", 2,
            "dept_name", "Sales",
            "location", "Building B"
        )));
        
        departments.add(new Row(Map.of(
            "dept_id", 3,
            "dept_name", "Marketing",
            "location", "Building C"
        )));
        
        departments.add(new Row(Map.of(
            "dept_id", 4,
            "dept_name", "HR",
            "location", "Building D"
        )));
        
        return departments;
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