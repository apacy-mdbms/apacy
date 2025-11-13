package com.apacy.storagemanager.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HashTable<K, V> {

    private List<Bucket<K, V>> buckets;
    private int bucketCapacity;

    public HashTable(int initialBucketCount, int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
        this.buckets = new ArrayList<>(initialBucketCount);
        for (int i = 0; i < initialBucketCount; i++)
            buckets.add(new Bucket<>(bucketCapacity,true));
    }

    public HashTable() {
        this(8, 4);
    }

    private int hash(K key) {
        if (key instanceof Integer i)
            return Math.abs(i.hashCode()) % bucketCount();
        else if (key instanceof Float f)
            return Math.abs(Float.floatToIntBits(f)) % bucketCount();
        else if (key instanceof String s)
            return Math.abs(s.hashCode()) % bucketCount();
        else if (key instanceof Character c)
            return Math.abs(Character.hashCode(c)) % bucketCount();
        else
            return Math.abs(key.hashCode()) % bucketCount();
    }

    public void insert(K key, V value) {
        int index = hash(key);
        Bucket<K, V> bucket = buckets.get(index);
        while (bucket.isFull()) {
            int nextIdx = bucket.getNextBucket();
            if (nextIdx == -1) {
                Bucket<K, V> newBucket = new Bucket<>(bucketCapacity,false);
                buckets.add(newBucket);
                int newIndex = buckets.size() - 1;
                bucket.setNextBucket(newIndex);
                bucket = newBucket;
                break;
            } else {
                bucket = buckets.get(nextIdx);
            }
        }
        bucket.insert(key, value);
    }

    public V get(K key) {
        int index = hash(key);
        Bucket<K, V> bucket = buckets.get(index);
        while (bucket != null) {
            V value = bucket.get(key);
            if (value != null)
                return value;
            int nextIdx = bucket.getNextBucket();
            if (nextIdx == -1)
                break;
            bucket = buckets.get(nextIdx);
        }
        return null;
    }

    public boolean remove(K key) {
        int index = hash(key);
        Bucket<K, V> bucket = buckets.get(index);
        while (bucket != null) {
            if (bucket.remove(key))
                return true;
            int nextIdx = bucket.getNextBucket();
            if (nextIdx == -1)
                break;
            bucket = buckets.get(nextIdx);
        }
        return false;
    }

    public int size() {
        int total = 0;
        for (Bucket<K, V> b : buckets)
            total += b.getEntries().size();
        return total;
    }

    public int bucketCount() {
        int count = 0;
        for (Bucket<K, V> bucket : buckets)
           count += bucket.startBucket ? 1 : 0;
        return count;
    }

    private void rehash(int newBucketCount) {
        if (newBucketCount < 1) return;

        List<Bucket.Entry<K, V>> allEntries = new ArrayList<>();
        for (Bucket<K, V> bucket : buckets)
            allEntries.addAll(bucket.getEntries());

        buckets = new ArrayList<>(newBucketCount);
        for (int i = 0; i < newBucketCount; i++)
            buckets.add(new Bucket<>(bucketCapacity,true));

        for (Bucket.Entry<K, V> e : allEntries)
            insert(e.key, e.value);

        System.out.println("Rehashed to " + newBucketCount + " buckets.");
    }

    public void expand(int count) {
        rehash(bucketCount()+count);
    }

    public void shrink(int count) {
        rehash(bucketCount()-count); 
    }

    public void printTable() {
        for (int i = 0; i < bucketCount(); i++)
            System.out.println("Bucket " + i + ": " + buckets.get(i));
    }

    // public static void main(String[] args) {
    //     System.out.println("=== HashTable Manual Test ===\n");

    //     // Basic insert/get/remove
    //     HashTable<Integer, String> ht = new HashTable<>(8, 4);
    //     ht.insert(10, "A");
    //     ht.insert(20, "B");
    //     ht.insert(30, "C");

    //     System.out.println("10 -> " + ht.get(10)); // expect A
    //     System.out.println("20 -> " + ht.get(20)); // expect B
    //     System.out.println("30 -> " + ht.get(30)); // expect C
    //     System.out.println("99 -> " + ht.get(99)); // expect null

    //     System.out.println("Remove 20: " + ht.remove(20));
    //     System.out.println("Get 20 after remove: " + ht.get(20));
    //     System.out.println("Remove 20 again: " + ht.remove(20));
    //     System.out.println("Size after removes: " + ht.size());
    //     System.out.println();

    //     // Force chaining
    //     System.out.println("=== Forcing chaining ===");
    //     HashTable<Integer, String> chainTest = new HashTable<>(1, 2);
    //     chainTest.insert(0, "v0");
    //     chainTest.insert(2, "v2");
    //     chainTest.insert(4, "v4");
    //     System.out.println("Size: " + chainTest.size());
    //     System.out.println("Bucket count: " + chainTest.bucketCount());
    //     System.out.println("Get 4 -> " + chainTest.get(4));
    //     chainTest.printTable();
    //     System.out.println();

    //     // Expand and shrink test
    //     System.out.println("=== Expand & Shrink Rehash Test ===");
    //     HashTable<Integer, String> big = new HashTable<>(2, 2);
    //     for (int i = 0; i < 50; i++) big.insert(i, "v" + i);
    //     System.out.println("Before expand - bucketCount: " + big.bucketCount());
    //     big.expand(10);
    //     System.out.println("After expand - bucketCount: " + big.bucketCount());
    //     for (int i = 0; i < 50; i++)
    //         if (!("v" + i).equals(big.get(i))) {
    //             System.out.println("Error: mismatch at " + i);
    //             break;
    //         }
    //     big.shrink(8);
    //     System.out.println("After shrink - bucketCount: " + big.bucketCount());
    //     System.out.println("All values still intact: OK\n");

    //     // Mixed key types
    //     System.out.println("=== Mixed key types ===");
    //     HashTable<String, Integer> hts = new HashTable<>(4, 3);
    //     hts.insert("alice", 1);
    //     hts.insert("bob", 2);
    //     System.out.println("alice -> " + hts.get("alice"));
    //     System.out.println("bob -> " + hts.get("bob"));

    //     HashTable<Character, String> htc = new HashTable<>(2, 2);
    //     htc.insert('A', "65");
    //     htc.insert('B', "66");
    //     System.out.println("A -> " + htc.get('A'));
    //     System.out.println("B -> " + htc.get('B'));

    //     HashTable<Float, String> htf = new HashTable<>(3, 2);
        // Random rnd = new Random(7);
        // for (int i = 0; i < 10; i++) {
        //     float f = rnd.nextFloat();
        //     htf.insert(f, "v" + i);
        //     System.out.println(f + " -> " + htf.get(f));
        // }
        // System.out.println("Float table size: " + htf.size());

        // // Visual table
        // System.out.println("\n=== Visual print ===");
        // HashTable<Integer, String> visual = new HashTable<>(2, 2);
        // for (int i = 0; i < 10; i++) visual.insert(i * 2, "v" + i);
        // visual.printTable();
    // }
}

