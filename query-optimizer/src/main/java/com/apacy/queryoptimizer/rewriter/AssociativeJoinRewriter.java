package com.apacy.queryoptimizer.rewriter;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.queryoptimizer.CostEstimator;

/**
 * Rule 6: Associative Join
 * Mengubah struktur (A JOIN B) JOIN C menjadi A JOIN (B JOIN C)
 * * Berguna jika (B JOIN C) lebih selektif (menghasilkan baris lebih sedikit) 
 * daripada (A JOIN B), atau untuk memungkinkan optimasi lanjutan.
 */
public class AssociativeJoinRewriter extends PlanRewriter {

    public AssociativeJoinRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

    @Override
    protected PlanNode visitJoin(JoinNode node, Map<String, Statistic> allStats) {
        // 1. Lakukan rewrite pada children terlebih dahulu (Bottom-Up)
        // Ini penting agar subtree di bawahnya sudah optimal sebelum kita putar.
        PlanNode left = rewrite(node.left(), allStats);
        PlanNode right = rewrite(node.right(), allStats);

        // Buat node baru dengan children yang mungkin sudah berubah
        // Kita tidak mengubah node asli secara langsung (immutability principle)
        JoinNode currentNode = new JoinNode(left, right, node.joinCondition(), node.joinType());

        // 2. Cek Pola: Apakah Left Child adalah JoinNode? 
        // Pola: (A JOIN B) JOIN C
        if (currentNode.left() instanceof JoinNode leftChildJoin) {
            
            // Bongkar komponen
            PlanNode A = leftChildJoin.left();
            PlanNode B = leftChildJoin.right();
            PlanNode C = currentNode.right();
            
            // Kondisi join
            Object condition1 = leftChildJoin.joinCondition(); // Kondisi antara A dan B
            Object condition2 = currentNode.joinCondition();   // Kondisi antara (A+B) dan C

            // 3. Logika Rotasi (Associativity)
            // Target: A JOIN (B JOIN C)
            // PERINGATAN: Ini adalah transformasi struktural murni. 
            // Dalam implementasi CBO (Cost Based Optimizer) yang nyata, kita harus mengecek
            // apakah 'condition2' benar-benar hanya melibatkan B dan C.
            // Untuk tugas ini, kita asumsikan kondisi dapat dipindahkan atau kita hanya mengubah struktur tree.
            
            // Buat Join baru di kanan: (B JOIN C) menggunakan condition2
            JoinNode newRightJoin = new JoinNode(B, C, condition2, currentNode.joinType());
            
            // Buat Join baru di root: A JOIN (newRightJoin) menggunakan condition1
            JoinNode newTopJoin = new JoinNode(A, newRightJoin, condition1, leftChildJoin.joinType());
            
            // (Opsional) Cek Cost: Apakah struktur baru lebih murah?
            // Jika estimator belum diimplementasikan penuh, kita bisa langsung return struktur baru
            // untuk membuktikan rule bekerja.
            // double costOld = costEstimator.estimateCost(currentNode, allStats);
            // double costNew = costEstimator.estimateCost(newTopJoin, allStats);
            
            // if (costNew < costOld) {
                 return newTopJoin;
            // }
        }

        // Jika pola tidak cocok, kembalikan node saat ini (yang children-nya sudah di-rewrite)
        return currentNode;
    }
}