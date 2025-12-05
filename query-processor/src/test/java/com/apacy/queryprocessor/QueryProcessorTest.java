package com.apacy.queryprocessor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.ast.expression.ColumnFactor;
import com.apacy.common.dto.ast.expression.ExpressionNode;
import com.apacy.common.dto.ast.expression.TermNode;
import com.apacy.common.dto.ast.join.JoinConditionNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.ast.where.ComparisonConditionNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.dto.plan.SortNode;
import com.apacy.common.interfaces.IConcurrencyControlManager;
import com.apacy.common.interfaces.IFailureRecoveryManager;
import com.apacy.common.interfaces.IQueryOptimizer;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.queryprocessor.mocks.MockConcurrencyControlManager;
import com.apacy.queryprocessor.mocks.MockFailureRecoveryManager;
import com.apacy.queryprocessor.mocks.MockQueryOptimizer;
import com.apacy.queryprocessor.mocks.MockStorageManager;

class QueryProcessorTest {
    
    private QueryProcessor queryProcessor;
    private MockQueryOptimizer mockQO;
    private MockStorageManager mockSM;
    
    @BeforeEach
    void setUp() {
        mockQO = new MockQueryOptimizer();
        mockSM = new MockStorageManager();
        IConcurrencyControlManager mockCCM = new MockConcurrencyControlManager();
        IFailureRecoveryManager mockFRM = new MockFailureRecoveryManager();

        queryProcessor = new QueryProcessor(mockQO, mockSM, mockCCM, mockFRM);
        try {
            queryProcessor.initialize();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    void testComponentName() {
        assertEquals("Query Processor", queryProcessor.getComponentName());
    }
    
    @Test
    void testInitialize() throws Exception {
        assertDoesNotThrow(() -> queryProcessor.initialize());
    }
    
    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> queryProcessor.shutdown());
    }

    @Test
    void test_executeQuery_SELECT_Success(){
        ExecutionResult result = queryProcessor.executeQuery("SELECT * FROM users");

        assertNotNull(result);
        assertTrue(result.success(), "SELECT harus sukses");

        assertEquals("SELECT executed successfully", result.message());
        assertEquals("SELECT", result.operation());
        
        assertEquals(5, result.affectedRows()); 
        assertNotNull(result.rows());
        assertEquals(5, result.rows().size());
        
        assertEquals("Naufarrel", result.rows().get(0).get("name"));
    }

    @Test
    void test_executeQuery_INSERT_Success(){
        ExecutionResult result = queryProcessor.executeQuery("INSERT INTO users (name) VALUES ('Budi')");

        assertNotNull(result);
        assertTrue(result.success(), "INSERT harus sukses");
        assertEquals("INSERT executed successfully", result.message());
        assertEquals(1, result.affectedRows()); 
    }

    @Test
    void test_executeQuery_UPDATE_Atomic_Success() {
        // Task 5: Verify Atomic Update
        ExecutionResult result = queryProcessor.executeQuery("UPDATE users SET name = 'John' WHERE id = 1");
        
        assertNotNull(result);
        assertTrue(result.success(), "UPDATE harus sukses");
        // MockSM returns 5 rows (all updated because mock plan lacks filter)
        assertEquals(5, result.affectedRows()); 
    }

    @Test
    void test_executeQuery_ComplexJoin_SortMerge() {
        // Task 8: Complex Query Test
        
        // 1. Override MockStats in MockSM
        mockSM.setOverrideStats("employees", new Statistic(60000, 1, 100, 100, Map.of(), Map.of()));
        mockSM.setOverrideStats("departments", new Statistic(60000, 1, 100, 100, Map.of(), Map.of()));
        
        // 2. Override MockQO to return a Join Plan that forces SortMergeJoin
        ScanNode scanEmp = new ScanNode("employees", "e");
        SortNode sortEmp = new SortNode(scanEmp, "dept_id", true); // Sorted input
        
        ScanNode scanDept = new ScanNode("departments", "d");
        SortNode sortDept = new SortNode(scanDept, "dept_id", true); // Sorted input
        
        // Join Condition: e.dept_id = d.dept_id
        ExpressionNode leftExpr = new ExpressionNode(new TermNode(new ColumnFactor("dept_id"), null), null);
        ExpressionNode rightExpr = new ExpressionNode(new TermNode(new ColumnFactor("dept_id"), null), null);
        ComparisonConditionNode joinCond = new ComparisonConditionNode(leftExpr, "=", rightExpr);
        
        JoinNode joinNode = new JoinNode(sortEmp, sortDept, joinCond, "INNER");
        SortNode finalSort = new SortNode(joinNode, "name", true);
        ProjectNode projectNode = new ProjectNode(finalSort, List.of("name", "dept_name"));
        
        ParsedQuery customPlan = new ParsedQuery(
            "SELECT", projectNode, List.of("employees", "departments"), 
            List.of("name", "dept_name"), null, null, null, "name", false, true
        );
        
        mockQO.setOverridePlan(customPlan);
        
        ExecutionResult result = queryProcessor.executeQuery("SELECT name, dept_name FROM employees JOIN departments ON employees.dept_id = departments.dept_id ORDER BY name");
        
        assertTrue(result.success());
        // Verify results are joined and sorted
        List<com.apacy.common.dto.Row> rows = result.rows();
        assertNotNull(rows);
        assertEquals(5, rows.size()); 
        
        // Verify Sort Order (Bayu, Farrel, Kinan, Naufarrel, Weka)
        assertEquals("Bayu", rows.get(0).get("name"));
        assertEquals("Weka", rows.get(4).get("name"));
        
        // Verify Join correctness (Bayu -> Sales)
        assertEquals("Sales", rows.get(0).get("dept_name"));
    }

    @Test
    void test_executeQuery_Fail_UnknownQuery(){
        ExecutionResult result = queryProcessor.executeQuery("JOGET DULU GAK SIE");

        assertNotNull(result);
        assertFalse(result.success(), "Query invalid harus gagal");
        assertNotNull(result.message()); 
    }
}
