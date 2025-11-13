package com.apacy.storagemanager.index;

import java.util.ArrayList;
import java.util.List;

public class Bucket<K, V> {

    private final int capacity;
    private final List<Entry<K, V>> entries;
    private int nextBucket;
    public boolean startBucket;

    public Bucket(int capacity, boolean startBucket) {
        this.capacity = capacity;
        this.entries = new ArrayList<>(capacity);
        this.nextBucket = -1;
        this.startBucket = startBucket;
    }

    public boolean isFull() {
        return entries.size() >= capacity;
    }

    public boolean insert(K key, V value) {
        if (isFull()) return false;
        entries.add(new Entry<>(key, value));
        return true;
    }

    public V get(K key) {
        for (Entry<K, V> e : entries)
            if (e.key.equals(key))
                return e.value;
        return null;
    }

    public boolean remove(K key) {
        return entries.removeIf(e -> e.key.equals(key));
    }

    public List<Entry<K, V>> getEntries() {
        return entries;
    }

    public int getNextBucket() {
        return nextBucket;
    }

    public void setNextBucket(int nextBucket) {
        this.nextBucket = nextBucket;
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean containsKey(K key) {
        for (Entry<K, V> e : entries)
            if (e.key.equals(key))
                return true;
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (Entry<K, V> e : entries)
            sb.append(e.key).append("=").append(e.value).append(", ");
        if (!entries.isEmpty())
            sb.setLength(sb.length() - 2);
        sb.append("]");
        return sb.toString() + " -> nextBucket=" + nextBucket;
    }

    public static class Entry<K, V> {
        public final K key;
        public final V value;
        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
}
