package com.apacy.queryprocessor.mocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.Statistic;
import com.apacy.common.enums.IndexType;
import com.apacy.common.interfaces.IStorageManager;

public class MockStorageManager implements IStorageManager {
    
    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        // Handle null case for backward compatibility
        if (dataRetrieval == null) {
            return getEmployeeDataLegacy();
        }
        
        String tableName = dataRetrieval.tableName();
        
        switch (tableName) {
            case "employees": return getEmployeeData();
            case "departments": return getDepartmentData();
            case "users": return getEmployeeDataLegacy(); // Mapping users -> legacy data
            default: return new ArrayList<>();
        }
    }
    
    private List<Row> getEmployeeDataLegacy() {
        List<Row> employees = new ArrayList<>();
        employees.add(new Row(Map.of("id", 1, "name", "Naufarrel", "salary", 20000)));
        employees.add(new Row(Map.of("id", 2, "name", "Weka", "salary", 30000)));
        employees.add(new Row(Map.of("id", 3, "name", "Kinan", "salary", 40000)));
        employees.add(new Row(Map.of("id", 4, "name", "Farrel", "salary", 50000)));
        employees.add(new Row(Map.of("id", 5, "name", "Bayu", "salary", 60000)));
        return employees;
    }
    
    private List<Row> getEmployeeData() {
        List<Row> employees = new ArrayList<>();
        employees.add(new Row(Map.of("id", 1, "name", "Naufarrel", "salary", 20000, "dept_id", 1)));
        employees.add(new Row(Map.of("id", 2, "name", "Weka", "salary", 30000, "dept_id", 2)));
        employees.add(new Row(Map.of("id", 3, "name", "Kinan", "salary", 40000, "dept_id", 1)));
        employees.add(new Row(Map.of("id", 4, "name", "Farrel", "salary", 50000, "dept_id", 3)));
        employees.add(new Row(Map.of("id", 5, "name", "Bayu", "salary", 60000, "dept_id", 2)));
        return employees;
    }
    
    private List<Row> getDepartmentData() {
        List<Row> departments = new ArrayList<>();
        departments.add(new Row(Map.of("dept_id", 1, "dept_name", "Engineering", "location", "Building A")));
        departments.add(new Row(Map.of("dept_id", 2, "dept_name", "Sales", "location", "Building B")));
        departments.add(new Row(Map.of("dept_id", 3, "dept_name", "Marketing", "location", "Building C")));
        departments.add(new Row(Map.of("dept_id", 4, "dept_name", "HR", "location", "Building D")));
        return departments;
    }

    @Override
    public int writeBlock(DataWrite dataWrite) {
        System.out.println("[MOCK-SM] writeBlock dipanggil untuk tabel: " + dataWrite.tableName());
        System.out.println("[MOCK-SM] Data baru: " + dataWrite.newData());
        return 1; // Simulasi 1 row inserted/updated
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        System.out.println("[MOCK-SM] deleteBlock dipanggil untuk tabel: " + dataDeletion.tableName());
        return 1; // Simulasi 1 row deleted
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        System.out.println("[MOCK-SM] setIndex dipanggil: " + table + "." + column);
    }

    @Override
    public Map<String, Statistic> getAllStats() {
        System.out.println("[MOCK-SM] getAllStats() dipanggil.");
        Map<String, Statistic> mockStats = new HashMap<>();

        mockStats.put("employees", new Statistic(
            5, 1, 120, 34,
            Map.of("id", 5, "dept_id", 3),
            Map.of("id", IndexType.Hash)
        ));

        mockStats.put("users", new Statistic(
            5, 1, 100, 40,
            Map.of("id", 5),
            Map.of()
        ));

        return mockStats;
    }

    @Override
    public void createTable(Schema schema) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}