package com.apacy.queryoptimizer.rewriter;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.queryoptimizer.CostEstimator;

/**
 * Rule 6: Associative Join
 * Mengubah struktur (A JOIN B) JOIN C menjadi A JOIN (B JOIN C)
 * Berguna jika (B JOIN C) lebih selektif (menghasilkan baris lebih sedikit)
 * daripada (A JOIN B), atau untuk memungkinkan optimasi lanjutan.
 */
public class AssociativeJoinRewriter extends PlanRewriter {

    public AssociativeJoinRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

    @Override
    protected PlanNode visitJoin(JoinNode node, Map<String, Statistic> allStats) {
        PlanNode left = rewrite(node.left(), allStats);
        PlanNode right = rewrite(node.right(), allStats);

        JoinNode currentNode = new JoinNode(left, right, node.joinCondition(), node.joinType());

        // Pola: (A JOIN B) JOIN C
        if (currentNode.left() instanceof JoinNode leftChildJoin) {

            // Bongkar komponen
            PlanNode A = leftChildJoin.left();
            PlanNode B = leftChildJoin.right();
            PlanNode C = currentNode.right();

            // Kondisi join
            Object condition1 = leftChildJoin.joinCondition(); // Kondisi antara A dan B
            Object condition2 = currentNode.joinCondition();   // Kondisi antara (A+B) dan C

            // Buat Join baru di kanan: (B JOIN C) menggunakan condition2
            JoinNode newRightJoin = new JoinNode(B, C, condition2, currentNode.joinType());

            // Buat Join baru di root: A JOIN (newRightJoin) menggunakan condition1
            JoinNode newTopJoin = new JoinNode(A, newRightJoin, condition1, leftChildJoin.joinType());

            double costOld = costEstimator.estimatePlanCost(currentNode, allStats);
            double costNew = costEstimator.estimatePlanCost(newTopJoin, allStats);
            // System.out.println(currentNode);
            // System.out.println(costOld);
            // System.out.println(newTopJoin);
            // System.out.println(costNew);

            if (costNew < costOld) {
                 return newTopJoin;
            }
        }

        // Jika pola tidak cocok, kembalikan node saat ini (yang children-nya sudah di-rewrite)
        return currentNode;
    }
}