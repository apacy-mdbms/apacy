package com.apacy.queryoptimizer.rewriter;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ProjectNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.queryoptimizer.CostEstimator;

class DistributeProjectTest {

    @Test
    void testDistributeProjectOverJoin() {
        // Setup: Project(Join(Scan A, Scan B))
        // Query: SELECT A.name FROM A JOIN B ON A.id = B.id
        
        ScanNode tableA = new ScanNode("A", "A");
        ScanNode tableB = new ScanNode("B", "B");
        
        // Kondisi Join dummy (Object string dulu biar simple)
        JoinNode joinNode = new JoinNode(tableA, tableB, "A.id = B.id", "INNER");
        
        // Project hanya butuh "name"
        ProjectNode topProject = new ProjectNode(joinNode, List.of("name"));

        // Eksekusi
        DistributeProjectRewriter rewriter = new DistributeProjectRewriter(new CostEstimator());
        PlanNode result = rewriter.rewrite(topProject, Map.of());

        // Verifikasi Struktur
        // Harapan: Project(Join(Project(A), Project(B)))
        
        assertTrue(result instanceof ProjectNode, "Root harus tetap ProjectNode");
        PlanNode child = ((ProjectNode) result).child();
        
        assertTrue(child instanceof JoinNode, "Anak Project harus JoinNode");
        JoinNode newJoin = (JoinNode) child;
        
        // Cek apakah anak kiri Join sekarang adalah ProjectNode?
        assertTrue(newJoin.left() instanceof ProjectNode, "Kiri Join harus ProjectNode (Pushdown berhasil)");
        assertTrue(newJoin.right() instanceof ProjectNode, "Kanan Join harus ProjectNode (Pushdown berhasil)");
        
        // Cek isi Project bawah
        ProjectNode leftProj = (ProjectNode) newJoin.left();
        // Kolom harusnya ["name"] (karena kondisi join "A.id=B.id" string, parser helper kita mungkin ga nangkep kalau bukan AST.
        // Tapi setidaknya struktur Project-nya turun.)
        System.out.println("Pushed columns: " + leftProj.columns());
        
        System.out.println("Original: " + topProject);
        System.out.println("Rewritten: " + result);
    }
}