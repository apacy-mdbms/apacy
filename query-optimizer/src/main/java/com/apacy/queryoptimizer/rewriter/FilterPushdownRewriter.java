package com.apacy.queryoptimizer.rewriter;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.queryoptimizer.CostEstimator;
import com.apacy.queryoptimizer.ast.where.WhereConditionNode;

// push down filter node
public class FilterPushdownRewriter extends PlanRewriter {

    public FilterPushdownRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

   @Override
    protected PlanNode visitFilter(FilterNode node, Map<String, Statistic> allStats) {
        PlanNode child = node.child();
        PlanNode rewrittenChild = rewrite(child, allStats);

        // Try pushdown over PROJECT
        if (rewrittenChild instanceof ProjectNode p) {
            if (canPredicateBePushed((WhereConditionNode)node.predicate(), p)) {
                return new ProjectNode(
                    new FilterNode(p.child(), node.predicate()),
                    p.columns()
                );
            }
        }

        if (child == rewrittenChild) return node;
        return new FilterNode(rewrittenChild, node.predicate());
    }

    protected boolean canPredicateBePushed(WhereConditionNode predicate, ProjectNode p) {
        // TODO: return true if attribute exists in child
        return true;
    }
}
