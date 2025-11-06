package com.apacy.common.interfaces;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Statistic;

/**
 * Kontrak untuk: Query Optimizer
 * Tugas: Menerjemahkan string query dan mengoptimasinya.
 */
public interface IQueryOptimizer {
    
    ParsedQuery parseQuery(String query);

    ParsedQuery optimizeQuery(ParsedQuery query, Statistic stats);

    double getCost(ParsedQuery query, Statistic stats);
}