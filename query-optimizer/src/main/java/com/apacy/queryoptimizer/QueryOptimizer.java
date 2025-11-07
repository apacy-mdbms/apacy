package com.apacy.queryoptimizer;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IQueryOptimizer;

import java.util.Map;

public class QueryOptimizer extends DBMSComponent implements IQueryOptimizer {

    private final QueryParser parser;
    private final HeuristicOptimizer optimizer;
    private final CostEstimator estimator;

    public QueryOptimizer() {
        super("Query Optimizer");
        // Inisialisasi helper-nya
        this.parser = new QueryParser();
        this.optimizer = new HeuristicOptimizer();
        this.estimator = new CostEstimator();
    }

    @Override
    public void initialize() throws Exception {
        // ... (Logika inisialisasi jika ada) ...
    }

    @Override
    public void shutdown() {
        // ... (Logika shutdown jika ada) ...
    }
    
    @Override
    public ParsedQuery parseQuery(String query) {
        // 5. Delegasikan tugas ke helper
        return this.parser.parse(query);
    }

    @Override
    public ParsedQuery optimizeQuery(ParsedQuery query, Map<String, Statistic> allStats) {
        // 5. Delegasikan tugas ke helper
        return this.optimizer.optimize(query, allStats);
    }

    @Override
    public double getCost(ParsedQuery query, Map<String, Statistic> allStats) {
        // 5. Delegasikan tugas ke helper
        // (CostEstimator Anda mengembalikan 'double', tapi interface minta 'int'.
        // Anda harus menyesuaikannya, misal dibulatkan atau di-cast)
        return this.estimator.estimate(query, allStats);
    }
}