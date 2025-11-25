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

    public List<Bucket<K, V>> getBuckets(){
        return buckets;
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

    public List<V> get(K key) {
        List<V> result = new ArrayList<>();

        int index = hash(key);
        Bucket<K, V> bucket = buckets.get(index);

        while (bucket != null) {

            // FIX: scan ALL entries in this bucket
            for (Bucket.Entry<K,V> e : bucket.getEntries()) {
                if (e.key.equals(key)) {
                    result.add(e.value);
                }
            }

            int nextIdx = bucket.getNextBucket();
            if (nextIdx == -1)
                break;

            bucket = buckets.get(nextIdx);
        }

        return result;
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

    public void printBuckets() {
        for (int i = 0; i < buckets.size(); i++) {
            Bucket<K, V> b = buckets.get(i);
            System.out.println("Bucket " + i);

            if (b == null) {
                System.out.println("  null");
                continue;
            }

            Bucket<K, V> curr = b;
            while (curr != null) {
                for (Bucket.Entry<K, V> e : curr.getEntries()) {
                    System.out.println("  key=" + e.key + " value=" + e.value);
                }

                int next = curr.getNextBucket();
                if (next == -1) break;
                curr = buckets.get(next);
            }
        }
    }

}

