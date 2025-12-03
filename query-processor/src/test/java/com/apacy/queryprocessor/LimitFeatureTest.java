package com.apacy.queryprocessor;

import com.apacy.common.dto.*;
import com.apacy.common.dto.plan.*;
import com.apacy.common.interfaces.*;
import com.apacy.queryprocessor.mocks.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class LimitFeatureTest {

    @Test
    void testLimitFeature() {
        IStorageManager sm = new MockStorageManager();
        IConcurrencyControlManager ccm = new MockConcurrencyControlManager();
        IFailureRecoveryManager frm = new MockFailureRecoveryManager();

        // Custom Mock Optimizer that returns a query with LIMIT 2
        IQueryOptimizer qo = new IQueryOptimizer() {
            @Override
            public ParsedQuery parseQuery(String query) {
                ScanNode scan = new ScanNode("employees", "e");
                ProjectNode project = new ProjectNode(scan, List.of("*"));
                
                // We want 2 rows, but MockStorageManager returns 5 rows for 'employees'
                return new ParsedQuery(
                    "SELECT", 
                    project, // planRoot without LimitNode (simulating optimizer not putting it in plan)
                    List.of("employees"), 
                    List.of("*"),
                    null, 
                    null, 
                    null, 
                    null, 
                    false, 
                    false,
                    2, // LIMIT 2
                    0, // OFFSET 0
                    null
                );
            }

            @Override
            public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
                return query;
            }

            @Override
            public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
                return 0;
            }
        };

        QueryProcessor qp = new QueryProcessor(qo, sm, ccm, frm);
        try {
            qp.initialize();
            ExecutionResult result = qp.executeQuery("SELECT * FROM employees LIMIT 2");
            
            assertTrue(result.success());
            assertEquals(2, result.rows().size(), "Should return exactly 2 rows due to LIMIT");
            
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    void testLimitAndOffsetFeature() {
        IStorageManager sm = new MockStorageManager();
        IConcurrencyControlManager ccm = new MockConcurrencyControlManager();
        IFailureRecoveryManager frm = new MockFailureRecoveryManager();

        // Custom Mock Optimizer that returns a query with LIMIT 2 OFFSET 1
        IQueryOptimizer qo = new IQueryOptimizer() {
            @Override
            public ParsedQuery parseQuery(String query) {
                ScanNode scan = new ScanNode("employees", "e");
                ProjectNode project = new ProjectNode(scan, List.of("*"));
                
                // MockStorageManager returns 5 rows: Naufarrel (id=1), Weka (id=2), Kinan (id=3), Farrel (id=4), Bayu (id=5)
                // OFFSET 1 should skip Naufarrel.
                // LIMIT 2 should take Weka, Kinan.
                return new ParsedQuery(
                    "SELECT", 
                    project, 
                    List.of("employees"), 
                    List.of("*"),
                    null, 
                    null, 
                    null, 
                    null, 
                    false, 
                    false,
                    2, // LIMIT 2
                    1, // OFFSET 1
                    null
                );
            }

            @Override
            public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
                return query;
            }

            @Override
            public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
                return 0;
            }
        };

        QueryProcessor qp = new QueryProcessor(qo, sm, ccm, frm);
        try {
            qp.initialize();
            ExecutionResult result = qp.executeQuery("SELECT * FROM employees LIMIT 2 OFFSET 1");
            
            assertTrue(result.success());
            assertEquals(2, result.rows().size(), "Should return 2 rows");
            
            // Check if the first row is 'Weka' (id=2) which is the 2nd row in mock data
            // MockStorageManager returns: Naufarrel, Weka, Kinan, Farrel, Bayu
            assertEquals("Weka", result.rows().get(0).get("name"));
            assertEquals("Kinan", result.rows().get(1).get("name"));
            
        } catch (Exception e) {
            fail(e);
        }
    }
}
