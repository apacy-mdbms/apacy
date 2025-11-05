package com.apacy.common.interfaces;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Statistic;

/**
 * Interface for Query Optimizer.
 * Handles SQL query parsing, optimization, and cost estimation.
 */
public interface IQueryOptimizer {
    
    /**
     * Parse a SQL query string into a structured ParsedQuery object.
     * 
     * @param sqlQuery The SQL query string to parse
     * @return ExecutionResult containing the parsed query or error information
     */
    ExecutionResult parseQuery(String sqlQuery);
    
    /**
     * Optimize a parsed query for better performance.
     * 
     * @param query The parsed query to optimize
     * @param statistics Database statistics for cost estimation
     * @return ExecutionResult containing the optimized query
     */
    ExecutionResult optimizeQuery(ParsedQuery query, Statistic statistics);
    
    /**
     * Estimate the execution cost of a query.
     * 
     * @param query The query to estimate cost for
     * @param statistics Database statistics for accurate estimation
     * @return ExecutionResult containing the estimated cost
     */
    ExecutionResult getCost(ParsedQuery query, Statistic statistics);
    
    /**
     * Validate the syntax and semantics of a SQL query.
     * 
     * @param sqlQuery The SQL query to validate
     * @return ExecutionResult indicating validation success or failure with details
     */
    ExecutionResult validateQuery(String sqlQuery);
    
    /**
     * Generate multiple execution plans for a query and select the best one.
     * 
     * @param query The parsed query to generate plans for
     * @param statistics Database statistics for plan evaluation
     * @return ExecutionResult containing the best execution plan
     */
    ExecutionResult generateExecutionPlan(ParsedQuery query, Statistic statistics);
    
    /**
     * Rewrite a query using optimization rules (e.g., predicate pushdown).
     * 
     * @param query The query to rewrite
     * @return ExecutionResult containing the rewritten query
     */
    ExecutionResult rewriteQuery(ParsedQuery query);
    
    /**
     * Estimate the selectivity of a WHERE clause condition.
     * 
     * @param whereClause The WHERE clause to analyze
     * @param statistics Table statistics for selectivity estimation
     * @return ExecutionResult containing selectivity estimate (0.0 to 1.0)
     */
    ExecutionResult estimateSelectivity(String whereClause, Statistic statistics);
    
    /**
     * Suggest indexes that could improve query performance.
     * 
     * @param query The query to analyze for index recommendations
     * @param statistics Current database statistics
     * @return ExecutionResult containing index recommendations
     */
    ExecutionResult suggestIndexes(ParsedQuery query, Statistic statistics);
}