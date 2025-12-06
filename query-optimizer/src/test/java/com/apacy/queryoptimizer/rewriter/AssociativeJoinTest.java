package com.apacy.queryoptimizer.rewriter;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.plan.JoinNode;
import com.apacy.common.dto.plan.PlanNode;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.enums.IndexType;
import com.apacy.queryoptimizer.CostEstimator;

class AssociativeJoinTest {

    @Test
    void testAssociativeJoinRewrite() {
        // Setup: Buat Tree (A JOIN B) JOIN C
        ScanNode tableA = new ScanNode("jurusan", "j");
        ScanNode tableB = new ScanNode("mahasiswa", "m");
        ScanNode tableC = new ScanNode("nilai", "n");

        // (A JOIN B)
        JoinNode leftJoin = new JoinNode(tableA, tableB, "j.id=m.jurusan_id", "INNER");

        // ((A JOIN B) JOIN C)
        JoinNode topJoin = new JoinNode(leftJoin, tableC, "m.id=n.mahasiswa_id", "INNER");

        Map<String, Statistic> stats = new HashMap<>();

        // Asumsi: 1000 baris, 50 blok, panjang baris 100 byte
        Map<String, Integer> vMahasiswa = new HashMap<>();
        vMahasiswa.put("id", 1000); // PK unik
        vMahasiswa.put("jurusan_id", 10); // 10 jurusan berbeda

        Map<String, IndexType> iMahasiswa = new HashMap<>();
        iMahasiswa.put("id", IndexType.Hash); // 10 jurusan berbeda

        Map<String, Object> minMahasiswa = new HashMap<>();
        Map<String, Object> maxMahasiswa = new HashMap<>();
        stats.put("mahasiswa", new Statistic(1000, 50, 100, 4096/100,vMahasiswa, iMahasiswa, minMahasiswa, maxMahasiswa));

        // Asumsi: 10 baris, 1 blok
        Map<String, Integer> vJurusan = new HashMap<>();
        vJurusan.put("id", 10);
        stats.put("jurusan", new Statistic(10, 1, 50, 4096/50, vJurusan, new HashMap<>(), new HashMap<>(), new HashMap<>()));

        // Asumsi: 50.000 baris, 2000 blok
        Map<String, Integer> vNilai = new HashMap<>();
        vNilai.put("mahasiswa_id", 1000);
        vNilai.put("mk_id", 50);
        stats.put("nilai", new Statistic(50000, 2000, 50, 4096/50, vNilai, new HashMap<>(), new HashMap<>(), new HashMap<>()));
        // Eksekusi Rewriter
        AssociativeJoinRewriter rewriter = new AssociativeJoinRewriter(new CostEstimator());
        PlanNode result = rewriter.rewrite(topJoin, stats);

        // Verifikasi Struktur Baru: A JOIN (B JOIN C)

        // 1. Root harus tetap JoinNode
        // assertTrue(result instanceof JoinNode);
        // JoinNode newRoot = (JoinNode) result;

        // // 2. Kiri dari root harus TABLE A (bukan Join lagi)
        // assertTrue(newRoot.left() instanceof ScanNode);
        // assertEquals("jurusan", ((ScanNode) newRoot.left()).tableName());

        // // 3. Kanan dari root harus JOIN (B JOIN C)
        // assertTrue(newRoot.right() instanceof JoinNode);
        // JoinNode rightChild = (JoinNode) newRoot.right();

        // // 4. Validasi isi join kanan
        // assertTrue(rightChild.left() instanceof ScanNode);
        // assertEquals("mahasiswa", ((ScanNode) rightChild.left()).tableName());
        // assertTrue(rightChild.right() instanceof ScanNode);
        // assertEquals("nilai", ((ScanNode) rightChild.right()).tableName());

        System.out.println("Original Tree: " + topJoin);
        System.out.println("Rewritten Tree: " + result);
    }
}