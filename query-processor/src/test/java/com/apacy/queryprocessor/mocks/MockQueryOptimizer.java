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
                List.of("name", "email"),                // targetColumns
                null,                                    // values
                null,                                    // joinConditions
                null,                                    // whereClause (AST)
                "name",                                  // orderByColumn
                false,                                   // isDescending
                false                                    // isOptimized
            );
        } else if (query.toLowerCase().contains("update")) {
            return new ParsedQuery(
                "UPDATE",                                // queryType
                List.of("users"),                        // targetTables
                null,                                    // targetColumns
                null,                                    // values
                null,                                    // joinConditions
                null,                                    // whereClause (AST)
                null,                                    // orderByColumn
                false,                                   // isDescending
                false                                    // isOptimized
            );
        }

        // TODO: Tambahkan skenario "INSERT", "DELETE", "CREATE", dll.

        // Default (jika query tidak dikenal)
        return null;
    }

    @Override
    public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
        return query;
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        return 1;
    }
}