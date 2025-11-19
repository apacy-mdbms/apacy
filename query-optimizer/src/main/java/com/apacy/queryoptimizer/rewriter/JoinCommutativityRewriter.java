package com.apacy.queryoptimizer.rewriter;

import java.util.Map;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.queryoptimizer.CostEstimator;

// Rewriter untuk sifat komutatif JOIN.
// E1 ⋈ E2 = E2 ⋈ E1
// Tukar lokasi E1 dan E2 kalau cardinality ruas kanan < cardinality ruas kiri.
public class JoinCommutativityRewriter extends PlanRewriter {

    public JoinCommutativityRewriter(CostEstimator costEstimator) {
        super(costEstimator);
    }

    @Override
    protected PlanNode visitJoin(JoinNode node, Map<String, Statistic> allStats) {
        PlanNode left = node.left();
        PlanNode newLeft = rewrite(left, allStats);

        PlanNode right = node.right();
        PlanNode newRight = rewrite(right, allStats);

        // Cek kalau komutatif (INNER JOIN atau NATURAL JOIN)
        String type = node.joinType().toUpperCase();
        if (type.equals("INNER") || type.equals("NATURAL")) {
            double leftCost = estimateCardinality(newLeft, allStats);
            double rightCost = estimateCardinality(newRight, allStats);

            // Taruh tabel yang cost nya lbh kecil di kiri
            if (rightCost < leftCost) {
                return new JoinNode(newRight, newLeft, node.joinCondition(), node.joinType());
            }
        }

        // Rekonstruksi kalau tdk ada penukaran atau perubahan children
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

    private double estimateCardinality(PlanNode node, Map<String, Statistic> allStats) {
        // Kalau node dari baca tabel secara langsung
        if (node instanceof ScanNode scan) {
            Statistic stat = allStats.get(scan.tableName());
            return stat != null ? stat.nr() : Double.MAX_VALUE;
        } 
        // Kalau node hasil join
        else if (node instanceof JoinNode join) {
            // Buat batas atas (Cartesian product), untuk perbandingan kira-kira mana yg lbh besar
            // Cth: (A JOIN B) JOIN C
            //      Kalau cardinality C < cardinality (A JOIN B)
            //                          (kira2 atau worst case scenario jlh row A * jlh row B),
            //      nanti ditukar jadi C JOIN (A JOIN B)
            double leftCard = estimateCardinality(join.left(), allStats);
            double rightCard = estimateCardinality(join.right(), allStats);
            if (leftCard == Double.MAX_VALUE || rightCard == Double.MAX_VALUE) {
                return Double.MAX_VALUE;
            }
            return leftCard * rightCard;
        }
        // Kalau node nya Filter atau Project, asumsi size sama dengan children di bawahnya (atau lbh kecil).
        // Rekursif ke bawah.
        if (!node.getChildren().isEmpty()) {
            // Rekursi untuk node dengan jlh children tunggal (Filter, Project, etc.)
            // Asumsi cardinality tdk meningkat
            return estimateCardinality(node.getChildren().get(0), allStats);
        }
        
        // Kalau size tidak diketahui, buat jadi MAX_VALUE utk cegah pertukaran.
        return Double.MAX_VALUE; 
    }
}
