package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IStorageManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                return new ArrayList<>();
        }
    }
    
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
        throw new UnsupportedOperationException("getStats not implemented yet");
    }
}