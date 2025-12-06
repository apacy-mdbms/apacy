package com.apacy.queryoptimizer;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IQueryOptimizer;

import java.text.ParseException;
import java.util.Map;

public class QueryOptimizer extends DBMSComponent implements IQueryOptimizer {

    private final QueryParser parser;
    private final HeuristicOptimizer optimizer;
    private final PhysicalPlanGenerator generator;
    private final CostEstimator estimator;

    public QueryOptimizer() {
        super("Query Optimizer");
        // Inisialisasi helper
        this.parser = new QueryParser();
        this.estimator = new CostEstimator();
        this.optimizer = new HeuristicOptimizer(estimator);
        this.generator = new PhysicalPlanGenerator(estimator);
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
        // System.out.println(query.planRoot());
        // System.out.println(getCost(query, allStats));
        ParsedQuery logicalOptimized = this.optimizer.optimize(query, allStats);
        // System.out.println(logicalOptimized.planRoot());
        // System.out.println(getCost(logicalOptimized, allStats));
        ParsedQuery physicalOptimized = this.generator.generate(logicalOptimized, allStats);
        // System.out.println(physicalOptimized.planRoot());
        // System.out.println(getCost(physicalOptimized, allStats));
        return physicalOptimized;
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        return this.estimator.estimatePlanCost(query.planRoot(), allStats);
    }
}