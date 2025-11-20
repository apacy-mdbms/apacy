package com.apacy.queryoptimizer.rewriter;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.queryoptimizer.CostEstimator;

class AssociativeJoinTest {

    @Test
    void testAssociativeJoinRewrite() {
        // Setup: Buat Tree (A JOIN B) JOIN C
        ScanNode tableA = new ScanNode("A", "A");
        ScanNode tableB = new ScanNode("B", "B");
        ScanNode tableC = new ScanNode("C", "C");

        // (A JOIN B)
        JoinNode leftJoin = new JoinNode(tableA, tableB, "A.id=B.id", "INNER");
        
        // ((A JOIN B) JOIN C)
        JoinNode topJoin = new JoinNode(leftJoin, tableC, "B.id=C.id", "INNER");

        // Eksekusi Rewriter
        AssociativeJoinRewriter rewriter = new AssociativeJoinRewriter(new CostEstimator());
        PlanNode result = rewriter.rewrite(topJoin, Map.of());

        // Verifikasi Struktur Baru: A JOIN (B JOIN C)
        
        // 1. Root harus tetap JoinNode
        assertTrue(result instanceof JoinNode);
        JoinNode newRoot = (JoinNode) result;

        // 2. Kiri dari root harus TABLE A (bukan Join lagi)
        assertTrue(newRoot.left() instanceof ScanNode);
        assertEquals("A", ((ScanNode) newRoot.left()).tableName());

        // 3. Kanan dari root harus JOIN (B JOIN C)
        assertTrue(newRoot.right() instanceof JoinNode);
        JoinNode rightChild = (JoinNode) newRoot.right();

        // 4. Validasi isi join kanan
        assertTrue(rightChild.left() instanceof ScanNode);
        assertEquals("B", ((ScanNode) rightChild.left()).tableName());
        assertTrue(rightChild.right() instanceof ScanNode);
        assertEquals("C", ((ScanNode) rightChild.right()).tableName());
        
        System.out.println("Original Tree: " + topJoin);
        System.out.println("Rewritten Tree: " + result);
    }
}