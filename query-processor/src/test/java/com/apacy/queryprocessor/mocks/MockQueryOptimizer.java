package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IQueryOptimizer;
import java.util.List;
import java.util.Map;

public class MockQueryOptimizer implements IQueryOptimizer {
    
    @Override
    public ParsedQuery parseQuery(String query) {
        System.out.println("[MOCK-QO] parseQuery dipanggil untuk: " + query.substring(0, Math.min(50, query.length())) + "...");
        
        if (query.toLowerCase().contains("select")) {
            return new ParsedQuery(
                "SELECT",                                // queryType
                List.of("users"),                        // targetTables
                List.of("*"),                           // targetColumns (gunakan * untuk semua kolom)
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
                List.of("name", "salary"),              // targetColumns untuk SET clause
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
                null,                                    // targetColumns (tidak diperlukan untuk DELETE)
                null,                                    // joinConditions
                null,                                    // whereClause (AST)
                null,                                    // orderByColumn
                false,                                   // isDescending
                false                                    // isOptimized
            );
        }
        
        // Default untuk query yang tidak dikenal
        System.out.println("[MOCK-QO] Query type tidak dikenal, mengembalikan null");
        return null;
    }

    @Override
    public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
        System.out.println("[MOCK-QO] optimizeQuery dipanggil untuk " + query.queryType() + 
                          " pada tabel: " + query.targetTables());
        System.out.println("[MOCK-QO] Menggunakan statistik untuk " + allStats.size() + " tabel");
        
        // Implementasi sederhana: hanya menandai bahwa query sudah dioptimasi
        return new ParsedQuery(
            query.queryType(),
            query.targetTables(),
            query.targetColumns(),
            query.joinConditions(),
            query.whereClause(),
            query.orderByColumn(),
            query.isDescending(),
            true // Mark sebagai dioptimasi
        );
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        System.out.println("[MOCK-QO] getCost dipanggil untuk " + query.queryType());
        
        // Estimasi cost sederhana berdasarkan tipe query
        switch (query.queryType().toUpperCase()) {
            case "SELECT":
                // Cost berdasarkan jumlah tabel dan apakah ada join
                int tableCount = query.targetTables() != null ? query.targetTables().size() : 1;
                return tableCount * 10.0; // Basic scan cost
                
            case "INSERT":
                return 5.0; // Insert operation cost
                
            case "UPDATE":
                return 15.0; // Update operation cost (lebih mahal karena perlu baca + tulis)
                
            case "DELETE":
                return 12.0; // Delete operation cost
                
            default:
                return 100.0; // Default high cost untuk operation yang tidak dikenal
        }
    }
}