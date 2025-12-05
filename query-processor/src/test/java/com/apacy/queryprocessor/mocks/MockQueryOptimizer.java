package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.dto.plan.*; 
import com.apacy.common.interfaces.IQueryOptimizer;
import java.util.List;
import java.util.Map;

public class MockQueryOptimizer implements IQueryOptimizer {

    private ParsedQuery overridePlan;

    public void setOverridePlan(ParsedQuery plan) {
        this.overridePlan = plan;
    }

    @Override
    public ParsedQuery parseQuery(String query) {
        if (overridePlan != null) {
            return overridePlan;
        }

        String lowerQuery = query.toLowerCase();

        if (lowerQuery.contains("select")) {
            ScanNode scan = new ScanNode("users", "u");
            ProjectNode project = new ProjectNode(scan, List.of("*"));
            return new ParsedQuery(
                "SELECT", project, List.of("users"), List.of("*"),
                null, null, null, null, false, false
            );

        } else if (lowerQuery.contains("insert")) {
            // Fixed: Removed 'email' to match MockStorageManager schema for 'users' (employees)
            List<String> cols = List.of("name", "salary");
            List<Object> vals = List.of("John Doe", 50000);
            ModifyNode insertNode = new ModifyNode("INSERT", null, "users", cols, vals);

            return new ParsedQuery(
                "INSERT", insertNode, List.of("users"), cols, vals,
                null, null, null, false, false
            );

        } else if (lowerQuery.contains("update")) {
            List<String> cols = List.of("name", "salary");
            List<Object> vals = List.of("John Smith", 60000);
            ScanNode scan = new ScanNode("users", "u");
            ModifyNode updateNode = new ModifyNode("UPDATE", scan, "users", cols, vals);

            return new ParsedQuery(
                "UPDATE", updateNode, List.of("users"), cols, vals,
                null, 
                "id = 1", 
                null, false, false
            );

        } else if (lowerQuery.contains("delete")) {
            ScanNode scan = new ScanNode("users", "u");
            ModifyNode deleteNode = new ModifyNode("DELETE", scan, "users", null, null);

            return new ParsedQuery(
                "DELETE", deleteNode, List.of("users"), null, null,
                null, 
                "id = 5", 
                null, false, false
            );
        }

        return null;
    }

    @Override
    public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
        if (overridePlan != null) {
            return overridePlan;
        }
        return new ParsedQuery(
            query.queryType(), query.planRoot(), query.targetTables(),
            query.targetColumns(), query.values(), query.joinConditions(),
            query.whereClause(), query.orderByColumn(), query.isDescending(),
            true 
        );
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        return 10.0;
    }
}
