package com.apacy.storagemanager;

import com.apacy.storagemanager.index.HashTable;
import org.junit.jupiter.api.Test;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.*;

class HashTableTest {

    @Test
    void testBasicInsertGetRemove() {
        HashTable<Integer, String> ht = new HashTable<>(8, 4);

        ht.insert(10, "A");
        ht.insert(20, "B");
        ht.insert(30, "C");

        assertEquals("A", ht.get(10));
        assertEquals("B", ht.get(20));
        assertEquals("C", ht.get(30));
        assertNull(ht.get(99));

        assertTrue(ht.remove(20));
        assertNull(ht.get(20));
        assertFalse(ht.remove(20));

        assertEquals(2, ht.size());
    }

    @Test
    void testForcingChainingByCapacity() {
        // 1 initial bucket, cap 2 => third insert must chain a new bucket
        HashTable<Integer, String> ht = new HashTable<>(1, 2);
        ht.insert(0, "v0");
        ht.insert(2, "v2"); // same bucket (hash % 1 == 0)
        ht.insert(4, "v4"); // triggers chain

        assertEquals(3, ht.size());
        // Bucket count must not increase when chaining happens
        assertTrue(ht.bucketCount() == 1);

        assertEquals("v0", ht.get(0));
        assertEquals("v2", ht.get(2));
        assertEquals("v4", ht.get(4));
    }

    @Test
    void testExpandAndShrinkRehashPreservesData() {
        HashTable<Integer, String> ht = new HashTable<>(2, 2);

        for (int i = 0; i < 50; i++) ht.insert(i, "v"+i);
        for (int i = 0; i < 50; i++) assertEquals("v"+i, ht.get(i));

        int beforeBuckets = ht.bucketCount();
        ht.expand(10); // rehash to more buckets
        assertTrue(ht.bucketCount() > beforeBuckets);
        for (int i = 0; i < 50; i++) assertEquals("v"+i, ht.get(i));

        ht.shrink(8); // rehash to fewer buckets (still >=1)
        assertTrue(ht.bucketCount() >= 1);
        for (int i = 0; i < 50; i++) assertEquals("v"+i, ht.get(i));
    }

    @Test
    void testMixedKeyTypes() {
        // String keys
        HashTable<String, Integer> hts = new HashTable<>(4, 3);
        hts.insert("alice", 1);
        hts.insert("bob", 2);
        assertEquals(1, hts.get("alice"));
        assertEquals(2, hts.get("bob"));

        // Character keys
        HashTable<Character, String> htc = new HashTable<>(2, 2);
        htc.insert('A', "65");
        htc.insert('B', "66");
        assertEquals("65", htc.get('A'));
        assertEquals("66", htc.get('B'));

        // Float keys (stress)
        HashTable<Float, String> htf = new HashTable<>(3, 2);
        Random rnd = new Random(7);
        for (int i = 0; i < 20; i++) {
            float f = rnd.nextFloat();
            htf.insert(f, "v"+i);
            assertEquals("v"+i, htf.get(f));
        }
        assertEquals(20, htf.size());
    }

    @Test
    void testPrintForVisual() {
        HashTable<Integer, String> ht = new HashTable<>(2, 2);
        for (int i = 0; i < 10; i++) ht.insert(i * 2, "v"+i);
        System.out.println("\n=== HashTable Buckets (visual) ===");
        ht.printTable();
    }
}
