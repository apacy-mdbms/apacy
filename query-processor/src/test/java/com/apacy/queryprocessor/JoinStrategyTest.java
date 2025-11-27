package com.apacy.queryprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.Row;
import com.apacy.queryprocessor.execution.JoinStrategy;
import com.apacy.queryprocessor.mocks.MockStorageManager;

@DisplayName("JoinStrategy Tests")
class JoinStrategyTest {
    
    private MockStorageManager mockStorageManager;
    private List<Row> employeeTable;
    private List<Row> departmentTable;
    private List<Row> emptyTable;
    
    @BeforeEach
    void setUp() {
        mockStorageManager = new MockStorageManager();
        
        // Get data from MockStorageManager
        employeeTable = mockStorageManager.readBlock(
            new DataRetrieval("employees", List.of("*"), null, false)
        );
        
        departmentTable = mockStorageManager.readBlock(
            new DataRetrieval("departments", List.of("*"), null, false)
        );
        
        emptyTable = new ArrayList<>();
    }
    
    // ========== Nested Loop Join Tests ==========
    
    @Test
    @DisplayName("Nested Loop Join - Basic Join on dept_id")
    void testNestedLoopJoin_Basic() {
        List<Row> result = JoinStrategy.nestedLoopJoin(employeeTable, departmentTable, "dept_id");
        
        // Should have 5 matches (all employees have matching departments)
        assertEquals(5, result.size());
        
        // Verify Naufarrel is joined with Engineering
        Row naufarrelRow = result.stream()
            .filter(r -> "Naufarrel".equals(r.get("name")))
            .findFirst()
            .orElse(null);
        assertNotNull(naufarrelRow);
        assertEquals("Engineering", naufarrelRow.get("dept_name"));
        assertEquals(1, naufarrelRow.get("dept_id"));
    }
    
    @Test
    @DisplayName("Nested Loop Join - Multiple Employees Same Department")
    void testNestedLoopJoin_MultipleEmployees() {
        List<Row> result = JoinStrategy.nestedLoopJoin(employeeTable, departmentTable, "dept_id");
        
        // Engineering (dept_id=1) has 2 employees: Naufarrel and Kinan
        long engineeringCount = result.stream()
            .filter(r -> "Engineering".equals(r.get("dept_name")))
            .count();
        assertEquals(2, engineeringCount);
        
        // Sales (dept_id=2) has 2 employees: Weka and Bayu
        long salesCount = result.stream()
            .filter(r -> "Sales".equals(r.get("dept_name")))
            .count();
        assertEquals(2, salesCount);
    }
    
    @Test
    @DisplayName("Nested Loop Join - With Null Values")
    void testNestedLoopJoin_WithNulls() {
        List<Row> employeeWithNull = new ArrayList<>(employeeTable);
        employeeWithNull.add(new Row(Map.of("id", 6, "name", "John", "salary", 25000)));
        
        List<Row> result = JoinStrategy.nestedLoopJoin(employeeWithNull, departmentTable, "dept_id");
        
        // Should still have 5 results (null dept_id is skipped)
        assertEquals(5, result.size());
    }
    
    @Test
    @DisplayName("Nested Loop Join - Empty Tables")
    void testNestedLoopJoin_EmptyTables() {
        assertTrue(JoinStrategy.nestedLoopJoin(emptyTable, departmentTable, "dept_id").isEmpty());
        assertTrue(JoinStrategy.nestedLoopJoin(employeeTable, emptyTable, "dept_id").isEmpty());
    }
    
    @Test
    @DisplayName("Nested Loop Join - No Matching Keys")
    void testNestedLoopJoin_NoMatches() {
        List<Row> noMatchDept = List.of(
            new Row(Map.of("dept_id", 99, "dept_name", "Unknown", "location", "Unknown"))
        );
        
        List<Row> result = JoinStrategy.nestedLoopJoin(employeeTable, noMatchDept, "dept_id");
        assertTrue(result.isEmpty());
    }
    
    @Test
    @DisplayName("Nested Loop Join - Null Arguments")
    void testNestedLoopJoin_NullArguments() {
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.nestedLoopJoin(null, departmentTable, "dept_id"));
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.nestedLoopJoin(employeeTable, null, "dept_id"));
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.nestedLoopJoin(employeeTable, departmentTable, null));
    }
    
    // ========== Hash Join Tests ==========
    
    @Test
    @DisplayName("Hash Join - Basic Join on dept_id")
    void testHashJoin_Basic() {
        List<Row> result = JoinStrategy.hashJoin(employeeTable, departmentTable, "dept_id");
        
        assertEquals(5, result.size());
        
        // Verify Weka is joined with Sales
        Row wekaRow = result.stream()
            .filter(r -> "Weka".equals(r.get("name")))
            .findFirst()
            .orElse(null);
        assertNotNull(wekaRow);
        assertEquals("Sales", wekaRow.get("dept_name"));
        assertEquals(2, wekaRow.get("dept_id"));
    }
    
    @Test
    @DisplayName("Hash Join - With Null Values")
    void testHashJoin_WithNulls() {
        List<Row> employeeWithNull = new ArrayList<>(employeeTable);
        employeeWithNull.add(new Row(Map.of("id", 6, "name", "Jane", "salary", 28000)));
        
        List<Row> result = JoinStrategy.hashJoin(employeeWithNull, departmentTable, "dept_id");
        
        assertEquals(5, result.size());
    }
    
    @Test
    @DisplayName("Hash Join - Empty Tables")
    void testHashJoin_EmptyTables() {
        assertTrue(JoinStrategy.hashJoin(emptyTable, departmentTable, "dept_id").isEmpty());
        assertTrue(JoinStrategy.hashJoin(employeeTable, emptyTable, "dept_id").isEmpty());
    }
    
    @Test
    @DisplayName("Hash Join - Null Arguments")
    void testHashJoin_NullArguments() {
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.hashJoin(null, departmentTable, "dept_id"));
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.hashJoin(employeeTable, null, "dept_id"));
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.hashJoin(employeeTable, departmentTable, (String) null));
    }
    
    // ========== Sort-Merge Join Tests ==========
    
    @Test
    @DisplayName("Sort-Merge Join - Basic Join on dept_id")
    void testSortMergeJoin_Basic() {
        List<Row> result = JoinStrategy.sortMergeJoin(employeeTable, departmentTable, "dept_id");
        
        assertEquals(5, result.size());
        
        // Verify Farrel is joined with Marketing
        Row farrelRow = result.stream()
            .filter(r -> "Farrel".equals(r.get("name")))
            .findFirst()
            .orElse(null);
        assertNotNull(farrelRow);
        assertEquals("Marketing", farrelRow.get("dept_name"));
        assertEquals(3, farrelRow.get("dept_id"));
    }
    
    @Test
    @DisplayName("Sort-Merge Join - With Null Values")
    void testSortMergeJoin_WithNulls() {
        List<Row> employeeWithNull = new ArrayList<>(employeeTable);
        employeeWithNull.add(new Row(Map.of("id", 6, "name", "Bob", "salary", 32000)));
        
        List<Row> result = JoinStrategy.sortMergeJoin(employeeWithNull, departmentTable, "dept_id");
        
        assertEquals(5, result.size());
    }
    
    @Test
    @DisplayName("Sort-Merge Join - Empty Tables")
    void testSortMergeJoin_EmptyTables() {
        assertTrue(JoinStrategy.sortMergeJoin(emptyTable, departmentTable, "dept_id").isEmpty());
        assertTrue(JoinStrategy.sortMergeJoin(employeeTable, emptyTable, "dept_id").isEmpty());
    }
    
    @Test
    @DisplayName("Sort-Merge Join - Null Arguments")
    void testSortMergeJoin_NullArguments() {
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.sortMergeJoin(null, departmentTable, "dept_id"));
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.sortMergeJoin(employeeTable, null, "dept_id"));
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.sortMergeJoin(employeeTable, departmentTable, null));
    }
    
    @Test
    @DisplayName("Sort-Merge Join - Duplicate Keys Cartesian Product")
    void testSortMergeJoin_DuplicateKeys() {
        // Engineering has 2 employees (Naufarrel, Kinan)
        List<Row> result = JoinStrategy.sortMergeJoin(employeeTable, departmentTable, "dept_id");
        
        long engineeringCount = result.stream()
            .filter(r -> "Engineering".equals(r.get("dept_name")))
            .count();
        assertEquals(2, engineeringCount);
    }
    
    // ========== Comparison Tests ==========
    
    @Test
    @DisplayName("All Join Strategies - Same Results")
    void testAllJoinStrategies_SameResults() {
        List<Row> nestedResult = JoinStrategy.nestedLoopJoin(employeeTable, departmentTable, "dept_id");
        List<Row> hashResult = JoinStrategy.hashJoin(employeeTable, departmentTable, "dept_id");
        List<Row> sortMergeResult = JoinStrategy.sortMergeJoin(employeeTable, departmentTable, "dept_id");
        
        // All should have same size
        assertEquals(5, nestedResult.size());
        assertEquals(5, hashResult.size());
        assertEquals(5, sortMergeResult.size());
        
        // All should contain same employee names
        Set<String> expectedNames = Set.of("Naufarrel", "Weka", "Kinan", "Farrel", "Bayu");
        
        Set<String> nestedNames = nestedResult.stream()
            .map(r -> (String) r.get("name"))
            .collect(java.util.stream.Collectors.toSet());
        
        Set<String> hashNames = hashResult.stream()
            .map(r -> (String) r.get("name"))
            .collect(java.util.stream.Collectors.toSet());
        
        Set<String> sortNames = sortMergeResult.stream()
            .map(r -> (String) r.get("name"))
            .collect(java.util.stream.Collectors.toSet());
        
        assertEquals(expectedNames, nestedNames);
        assertEquals(expectedNames, hashNames);
        assertEquals(expectedNames, sortNames);
    }

    // ========== Cartesian Join Tests ==========

    @Test
    @DisplayName("Cartesian Join - Basic")
    void testCartesianJoin_Basic() {
        List<Row> result = JoinStrategy.cartesianJoin(employeeTable, departmentTable);
        
        // Employee table has 5 rows, Department table has 4 rows
        // Cartesian product should have 5 * 4 = 20 rows
        assertEquals(20, result.size());
    }

    @Test
    @DisplayName("Cartesian Join - Empty Table")
    void testCartesianJoin_EmptyTable() {
        List<Row> result1 = JoinStrategy.cartesianJoin(employeeTable, emptyTable);
        assertTrue(result1.isEmpty());

        List<Row> result2 = JoinStrategy.cartesianJoin(emptyTable, departmentTable);
        assertTrue(result2.isEmpty());
    }

    @Test
    @DisplayName("Cartesian Join - Null Arguments")
    void testCartesianJoin_NullArguments() {
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.cartesianJoin(null, departmentTable));
        assertThrows(IllegalArgumentException.class, 
            () -> JoinStrategy.cartesianJoin(employeeTable, null));
    }
}