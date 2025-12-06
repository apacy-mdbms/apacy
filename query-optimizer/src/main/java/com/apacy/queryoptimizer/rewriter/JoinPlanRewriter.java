package com.apacy.queryoptimizer.rewriter;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.enums.JoinAlgorithm;
import com.apacy.queryoptimizer.CostEstimator;

public class JoinPlanRewriter extends PlanRewriter {

    public JoinPlanRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

    @Override
    protected PlanNode visitJoin(JoinNode node, Map<String, Statistic> allStats) {
        PlanNode left = node.left();
        PlanNode newLeft = rewrite(left, allStats);

        PlanNode right = node.right();
        PlanNode newRight = rewrite(right, allStats);

        JoinNode ret;
        // Rekonstruksi kalau tdk ada penukaran atau perubahan children
        if (left == newLeft && right == newRight) {
            ret =  node;
        } else if (left != newLeft) {
            ret =  new JoinNode(newLeft, right, node.joinCondition(), node.joinType());
        } else if (right != newRight) {
            ret = new JoinNode(left, newRight, node.joinCondition(), node.joinType());
        } else {
            ret =  new JoinNode(newLeft, newRight, node.joinCondition(), node.joinType());
        }

        double nested = costEstimator.costJoinNestedLoop(ret, allStats);
        double merge = costEstimator.costJoinSortMerge(ret, allStats);
        double hash = costEstimator.costJoinHash(ret, allStats);

        JoinAlgorithm strat;
        if (nested <= merge && nested <= hash) {
            strat = JoinAlgorithm.NESTED_LOOP;
        } else if (merge <= nested && merge <= hash) {
            strat = JoinAlgorithm.SORT_MERGE;
        } else {
            strat = JoinAlgorithm.HASH;
        }
        return new JoinNode(ret.left(), ret.right(), ret.joinCondition(), ret.joinType(), strat);
    }
}
