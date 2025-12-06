package com.apacy.queryoptimizer;

import java.util.List;
import java.util.Map;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.queryoptimizer.rewriter.JoinPlanRewriter;
import com.apacy.queryoptimizer.rewriter.PlanRewriter;

public class PhysicalPlanGenerator {

    private List<PlanRewriter> rules = List.of();

    PhysicalPlanGenerator(CostEstimator costEstimator) {
        rules = List.of(
            new JoinPlanRewriter(costEstimator)
        );
    }

    public ParsedQuery generate(ParsedQuery query, Map<String, Statistic> allStats) {
        PlanNode curr = query.planRoot();
        for (PlanRewriter rule : rules) {
            PlanNode rewritten = rule.rewrite(curr, allStats);
            if (!rewritten.equals(curr)) {
                System.out.println(rule.getClass().getName());
                System.out.println(rewritten);
                curr = rewritten;
            }
        }
        return new ParsedQuery(
            query.queryType(),
            curr,
            query.targetTables(),
            query.targetColumns(),
            query.values(),
            query.joinConditions(),
            query.whereClause(),
            query.orderByColumn(),
            query.isDescending(),
            true);
    }
}
