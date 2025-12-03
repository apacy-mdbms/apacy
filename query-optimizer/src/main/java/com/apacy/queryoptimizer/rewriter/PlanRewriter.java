package com.apacy.queryoptimizer.rewriter;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.CartesianNode;
import com.apacy.common.dto.plan.FilterNode;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.LimitNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.dto.plan.SortNode;
import com.apacy.queryoptimizer.CostEstimator;


// Base PlanRewriter class using Path Copying
// Extend to apply optimization rules
public class PlanRewriter {

    protected CostEstimator costEstimator;

    public PlanRewriter(CostEstimator costEstimator) {
        this.costEstimator = costEstimator;
    }

    public PlanNode rewrite(PlanNode node, Map<String, Statistic> allStats) {
        if (node instanceof FilterNode n) {
            return visitFilter(n, allStats);
        } else if (node instanceof ProjectNode n) {
            return visitProject(n, allStats);
        } else if (node instanceof JoinNode n) {
            return visitJoin(n, allStats);
        } else if (node instanceof ScanNode n) {
            return visitScan(n, allStats);
        } else if (node instanceof SortNode n) {
            return visitSort(n, allStats);
        } else if (node instanceof LimitNode n) {
            return visitLimit(n, allStats);
        } else if (node instanceof CartesianNode n) {
            return visitCartesian(n, allStats);
        }
        // add other type of PlanNode if needed

        // else dont process
        return node;

    }

    protected PlanNode visitFilter(FilterNode node, Map<String, Statistic> allStats) {
        PlanNode child = node.child();
        PlanNode newChild = rewrite(child, allStats);

        // no change, return original
        if (child == newChild) {
            return node;
        }
        // optimization happened in lower depth, do Path Copying
        return new FilterNode(newChild, node.predicate());
    }

    protected PlanNode visitJoin(JoinNode node, Map<String, Statistic> allStats) {
        PlanNode left = node.left();
        PlanNode newLeft = rewrite(left, allStats);

        PlanNode right = node.right();
        PlanNode newRight = rewrite(right, allStats);

        if (left == newLeft && right == newRight) {
            return node;
        } else if (left != newLeft) {
            return new JoinNode(newLeft, right, node.joinCondition(), node.joinType());
        } else if (right != newRight) {
            return new JoinNode(left, newRight, node.joinCondition(), node.joinType());
        } else {
            return new JoinNode(newLeft, newRight, node.joinCondition(), node.joinType());
        }

    }

    protected PlanNode visitCartesian(CartesianNode node, Map<String, Statistic> allStats) {
        PlanNode left = node.left();
        PlanNode newLeft = rewrite(left, allStats);

        PlanNode right = node.right();
        PlanNode newRight = rewrite(right, allStats);

        if (left == newLeft && right == newRight) {
            return node;
        } else if (left != newLeft) {
            return new CartesianNode(newLeft, right);
        } else if (right != newRight) {
            return new CartesianNode(left, newRight);
        } else {
            return new CartesianNode(newLeft, newRight);
        }

    }

    protected PlanNode visitProject(ProjectNode node, Map<String, Statistic> allStats) {
        PlanNode child = node.child();
        PlanNode newChild = rewrite(child, allStats);

        // no change, return original
        if (child == newChild) {
            return node;
        }
        // optimization happened in lower depth, do Path Copying
        return new ProjectNode(newChild, node.columns());
    }

    protected PlanNode visitSort(SortNode node, Map<String, Statistic> allStats) {
        PlanNode child = node.child();
        PlanNode newChild = rewrite(child, allStats);

        // no change, return original
        if (child == newChild) {
            return node;
        }
        // optimization happened in lower depth, do Path Copying
        return new SortNode(newChild, node.sortColumn(), node.ascending());
    }

    protected PlanNode visitLimit(LimitNode node, Map<String, Statistic> allStats) {
        PlanNode child = node.child();
        PlanNode newChild = rewrite(child, allStats);

        // no change, return original
        if (child == newChild) {
            return node;
        }
        // optimization happened in lower depth, do Path Copying
        return new LimitNode(newChild, node.limit(), node.offset());
    }

    protected PlanNode visitScan(ScanNode node, Map<String, Statistic> allStats) {
        return node;
    }

}
