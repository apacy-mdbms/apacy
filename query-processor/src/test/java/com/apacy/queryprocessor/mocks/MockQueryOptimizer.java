package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IQueryOptimizer;
import java.util.List;
import java.util.Map;

public class MockQueryOptimizer implements IQueryOptimizer {

    @Override
    public ParsedQuery parseQuery(String query) {
        if (query.toLowerCase().contains("select")) {
            return new ParsedQuery(
                "SELECT",                                // queryType
                List.of("users"),                        // targetTables
                List.of("*"),                           // targetColumns
                null,                                    // values (for SELECT, this is null)
                null,                                    // joinConditions
                null,                                    // whereClause (AST)
                null,                                    // orderByColumn
                false,                                   // isDescending
                false                                    // isOptimized
            );
        } else if (query.toLowerCase().contains("insert")) {
            return new ParsedQuery(
                "INSERT",                                // queryType
                List.of("users"),                        // targetTables
                List.of("name", "email", "salary"),     // targetColumns
                List.of("John Doe", "john@example.com", 50000), // values (sample INSERT values)
                null,                                    // joinConditions
                null,                                    // whereClause (AST)
                null,                                    // orderByColumn
                false,                                   // isDescending
                false                                    // isOptimized
            );
        } else if (query.toLowerCase().contains("update")) {
            return new ParsedQuery(
                "UPDATE",                                // queryType
                List.of("users"),                        // targetTables
                List.of("name", "salary"),              // targetColumns (SET clause)
                List.of("John Smith", 60000),           // values (new values for UPDATE)
                null,                                    // joinConditions
                null,                                    // whereClause (AST)
                null,                                    // orderByColumn
                false,                                   // isDescending
                false                                    // isOptimized
            );
        } else if (query.toLowerCase().contains("delete")) {
            return new ParsedQuery(
                "DELETE",                                // queryType
                List.of("users"),                        // targetTables
                null,                                    // targetColumns (not needed for DELETE)
                null,                                    // values (not needed for DELETE)
                null,                                    // joinConditions
                null,                                    // whereClause (AST)
                null,                                    // orderByColumn
                false,                                   // isDescending
                false                                    // isOptimized
            );
        }

        // Default (jika query tidak dikenal)
        return null;
    }

    @Override
    public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
        System.out.println("[MOCK-QO] optimizeQuery dipanggil untuk " + query.queryType() + 
                          " pada tabel: " + query.targetTables());
        System.out.println("[MOCK-QO] Menggunakan statistik untuk " + allStats.size() + " tabel");
        
        // Return an optimized version (mark isOptimized as true)
        return new ParsedQuery(
            query.queryType(),
            query.targetTables(),
            query.targetColumns(),
            query.values(),
            query.joinConditions(),
            query.whereClause(),
            query.orderByColumn(),
            query.isDescending(),
            true // Mark as optimized
        );
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        System.out.println("[MOCK-QO] getCost dipanggil untuk " + query.queryType());
        
        // Simple cost estimation based on query type
        switch (query.queryType().toUpperCase()) {
            case "SELECT":
                int tableCount = query.targetTables() != null ? query.targetTables().size() : 1;
                return tableCount * 10.0; // Basic scan cost
                
            case "INSERT":
                return 5.0; // Insert operation cost
                
            case "UPDATE":
                return 15.0; // Update operation cost (more expensive due to read + write)
                
            case "DELETE":
                return 12.0; // Delete operation cost
                
            default:
                return 100.0; // Default high cost for unknown operations
        }
    }
}