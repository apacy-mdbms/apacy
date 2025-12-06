package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Statistic;
import com.apacy.common.interfaces.IQueryOptimizer;

import java.util.Collections;
import java.util.Map;

/**
 * Mock implementasi dari IQueryOptimizer untuk keperluan testing Query Processor.
 */
public class MockQueryOptimizer implements IQueryOptimizer {

    // --- Configuration (Stubbing) ---
    private ParsedQuery parsedQueryToReturn;
    private double costToReturn = 10.0;

    // --- State (Spying) ---
    private String lastParsedQueryString;
    private ParsedQuery lastOptimizedQuery;
    private int parseQueryCallCount = 0;
    private int optimizeQueryCallCount = 0;
    private int getCostCallCount = 0;

    /**
     * Mengatur ParsedQuery spesifik yang akan dikembalikan oleh parseQuery().
     * Jika null, parseQuery() akan menghasilkan objek default sederhana.
     */
    public void setParsedQueryToReturn(ParsedQuery parsedQueryToReturn) {
        this.parsedQueryToReturn = parsedQueryToReturn;
    }

    /**
     * Mengatur cost yang akan dikembalikan oleh getCost().
     */
    public void setCostToReturn(double costToReturn) {
        this.costToReturn = costToReturn;
    }

    @Override
    public ParsedQuery parseQuery(String query) {
        this.parseQueryCallCount++;
        this.lastParsedQueryString = query;

        if (this.parsedQueryToReturn != null) {
            return this.parsedQueryToReturn;
        }

        // Fallback: Kembalikan objek ParsedQuery minimal yang valid
        // Deteksi tipe query sederhana (SELECT, INSERT, dll) dari string
        String detectedType = "UNKNOWN";
        if (query != null && !query.trim().isEmpty()) {
            detectedType = query.trim().split("\\s+")[0].toUpperCase();
        }

        return new ParsedQuery(
            detectedType,
            null, // PlanNode root (biasanya null jika tidak diset manual di mock)
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            false, // isDescending
            false, // isOptimized (belum dioptimasi saat parsing)
            null,  // limit
            null,  // offset
            Collections.emptyMap()
        );
    }

    @Override
    public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
        this.optimizeQueryCallCount++;
        
        // Simulasikan optimasi dengan mengembalikan salinan query yang flag isOptimized = true
        ParsedQuery optimizedQuery = new ParsedQuery(
            query.queryType(),
            query.planRoot(),
            query.targetTables(),
            query.targetColumns(),
            query.values(),
            query.joinConditions(),
            query.whereClause(),
            query.orderByColumn(),
            query.isDescending(),
            true, // FLAG UPDATED: isOptimized = true
            query.limit(),
            query.offset(),
            query.aliasMap()
        );

        this.lastOptimizedQuery = optimizedQuery;
        return optimizedQuery;
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        this.getCostCallCount++;
        return this.costToReturn;
    }

    // --- Helper Methods untuk Verifikasi Test ---

    public String getLastParsedQueryString() {
        return lastParsedQueryString;
    }

    public ParsedQuery getLastOptimizedQuery() {
        return lastOptimizedQuery;
    }

    public int getParseQueryCallCount() {
        return parseQueryCallCount;
    }

    public int getOptimizeQueryCallCount() {
        return optimizeQueryCallCount;
    }
    
    public int getGetCostCallCount() {
        return getCostCallCount;
    }

    public void reset() {
        this.lastParsedQueryString = null;
        this.lastOptimizedQuery = null;
        this.parseQueryCallCount = 0;
        this.optimizeQueryCallCount = 0;
        this.getCostCallCount = 0;
        this.parsedQueryToReturn = null;
        this.costToReturn = 10.0;
    }
}