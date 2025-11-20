package com.apacy.queryoptimizer.rewriter;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.queryoptimizer.CostEstimator;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinCommutativityRewriterTest {

    @Test
    // A JOIN B -> B JOIN A
    void testSwapWhenRightIsSmaller() {
        // Tabel A: 1000 row
        // Tabel B: 5 row
        // B(5) < A(1000) -> tukar jadi B JOIN A.
        Statistic statA = new Statistic(1000, 100, 50, 10,
                                        Collections.emptyMap(), Collections.emptyMap());
        Statistic statB = new Statistic(5, 1, 50, 10,
                                        Collections.emptyMap(), Collections.emptyMap());
        
        Map<String, Statistic> stats = Map.of(
            "A", statA,
            "B", statB
        );

        ScanNode scanA = new ScanNode("A", "a");
        ScanNode scanB = new ScanNode("B", "b");
        
        JoinNode originalJoin = new JoinNode(scanA, scanB, null, "INNER");

        JoinCommutativityRewriter rewriter = new JoinCommutativityRewriter(new CostEstimator());
        PlanNode result = rewriter.rewrite(originalJoin, stats);

        assertTrue(result instanceof JoinNode);
        JoinNode resultJoin = (JoinNode) result;

        // Cek kalau sudah ditukar
        assertTrue(resultJoin.left() instanceof ScanNode);
        assertEquals("B", ((ScanNode) resultJoin.left()).tableName());
        
        assertTrue(resultJoin.right() instanceof ScanNode);
        assertEquals("A", ((ScanNode) resultJoin.right()).tableName());
    }

    @Test
    // A JOIN B -> A JOIN B
    void testNoSwapWhenLeftIsSmaller() {
        // Tabel A: 5 row
        // Tabel B: 1000 row
        // B(1000) > A(5) -> tidak tukar.
        Statistic statA = new Statistic(5, 1, 50, 10, Collections.emptyMap(), Collections.emptyMap());
        Statistic statB = new Statistic(1000, 100, 50, 10, Collections.emptyMap(), Collections.emptyMap());
        
        Map<String, Statistic> stats = Map.of(
            "A", statA,
            "B", statB
        );

        ScanNode scanA = new ScanNode("A", "a");
        ScanNode scanB = new ScanNode("B", "b");
        
        JoinNode originalJoin = new JoinNode(scanA, scanB, null, "INNER");

        JoinCommutativityRewriter rewriter = new JoinCommutativityRewriter(new CostEstimator());
        PlanNode result = rewriter.rewrite(originalJoin, stats);

        assertTrue(result instanceof JoinNode);
        JoinNode resultJoin = (JoinNode) result;

        // Cek kalau tidak ditukar
        assertEquals("A", ((ScanNode) resultJoin.left()).tableName());
        assertEquals("B", ((ScanNode) resultJoin.right()).tableName());
    }

    @Test
    // (A JOIN B) JOIN (C JOIN (D JOIN E)) -> (C JOIN (E JOIN D)) JOIN (A JOIN B)
    void testComplexNestedJoin() {
        // A: 1000
        // B: 2000
        // Subtree kiri cost 2.000.000. (B(2000) > A(1000) -> tidak tukar).
        
        // D: 100
        // E: 10
        // Inner Right Join (D JOIN E) cost 1000. (E(10) < D(100) -> Tukar jadi E JOIN D)
        
        // C: 500
        // Middle Right Join (C JOIN (E JOIN D)) cost 500 * 1000 = 500.000.
        //      (E JOIN D)(1000) (kanan) > C(500) (kiri) -> tidak tukar.
        
        // Kiri: (A JOIN B) -> Cost 2.000.000
        // Kanan: (C JOIN (E JOIN D)) -> Cost 500.000
        // (C JOIN (E JOIN D))(500.000) < (A JOIN B)(2.000.000)
        //      -> Tukar jadi (C JOIN (E JOIN D)) JOIN (A JOIN B)

        Statistic statA = new Statistic(1000, 100, 50, 10, Collections.emptyMap(), Collections.emptyMap());
        Statistic statB = new Statistic(2000, 100, 50, 10, Collections.emptyMap(), Collections.emptyMap());
        Statistic statC = new Statistic(500, 100, 50, 10, Collections.emptyMap(), Collections.emptyMap());
        Statistic statD = new Statistic(100, 100, 50, 10, Collections.emptyMap(), Collections.emptyMap());
        Statistic statE = new Statistic(10, 100, 50, 10, Collections.emptyMap(), Collections.emptyMap());

        Map<String, Statistic> stats = Map.of(
            "A", statA, "B", statB, "C", statC, "D", statD, "E", statE
        );

        ScanNode a = new ScanNode("A", "a");
        ScanNode b = new ScanNode("B", "b");
        ScanNode c = new ScanNode("C", "c");
        ScanNode d = new ScanNode("D", "d");
        ScanNode e = new ScanNode("E", "e");

        // Bangun tree: (A JOIN B) JOIN (C JOIN (D JOIN E))
        JoinNode joinAB = new JoinNode(a, b, null, "INNER");
        JoinNode joinDE = new JoinNode(d, e, null, "INNER");
        JoinNode joinCDE = new JoinNode(c, joinDE, null, "INNER");
        JoinNode topJoin = new JoinNode(joinAB, joinCDE, null, "INNER");

        JoinCommutativityRewriter rewriter = new JoinCommutativityRewriter(new CostEstimator());
        PlanNode result = rewriter.rewrite(topJoin, stats);

        assertTrue(result instanceof JoinNode);
        JoinNode top = (JoinNode) result;

        // 1. Cek pertukaran top
        // Kiri: subtree C-D-E
        assertTrue(top.left() instanceof JoinNode); 
        // Kanan: subtree A-B
        assertTrue(top.right() instanceof JoinNode);
        
        JoinNode topLeft = (JoinNode) top.left(); // C JOIN (E JOIN D)
        JoinNode topRight = (JoinNode) top.right(); // A JOIN B

        // 2. Cek subtree A-B (Tidak tukar karena B(2000) (kanan) > A(1000) (kiri))
        assertEquals("A", ((ScanNode) topRight.left()).tableName());
        assertEquals("B", ((ScanNode) topRight.right()).tableName());

        // 3. Cek subtree C-D-E
        // Structure: C JOIN (E JOIN D)
        assertEquals("C", ((ScanNode) topLeft.left()).tableName());
        assertTrue(topLeft.right() instanceof JoinNode);
        
        // 4. Cek subtree D-E (Tukar karena E(10) (kanan) < D(100) (kiri))
        JoinNode innerDE = (JoinNode) topLeft.right();
        assertEquals("E", ((ScanNode) innerDE.left()).tableName());
        assertEquals("D", ((ScanNode) innerDE.right()).tableName());
    }
}
