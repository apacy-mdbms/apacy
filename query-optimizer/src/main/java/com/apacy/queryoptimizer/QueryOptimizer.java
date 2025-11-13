package com.apacy.queryoptimizer;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IQueryOptimizer;

import java.text.ParseException;
import java.util.Map;

public class QueryOptimizer extends DBMSComponent implements IQueryOptimizer {

    private final QueryParser parser;
    private final HeuristicOptimizer optimizer;
    private final CostEstimator estimator;

    public QueryOptimizer() {
        super("Query Optimizer");
        // Inisialisasi helper
        this.parser = new QueryParser();
        this.optimizer = new HeuristicOptimizer();
        this.estimator = new CostEstimator();
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public ParsedQuery parseQuery(String query) {
        try {
            return this.parser.parse(query);
        } catch (ParseException pe)  {
            return null;
        }
    }

    @Override
    public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
        return this.optimizer.optimize(query, allStats);
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        return this.estimator.estimate(query, allStats);
    }
}