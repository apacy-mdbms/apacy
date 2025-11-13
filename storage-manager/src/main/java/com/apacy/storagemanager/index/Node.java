package com.apacy.storagemanager.index;

import java.util.ArrayList;
import java.util.List;

public class Node<K extends Comparable<K>, V> {

    private boolean isLeaf;
    private final List<K> keys;
    private final List<Integer> children; // indices of child nodes
    private final List<V> values;         // only for leaves
    private final int maxKeys;

    public Node(int maxKeys, boolean isLeaf) {
        this.maxKeys = maxKeys;
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        this.values = isLeaf ? new ArrayList<>() : null;
    }

    public boolean isLeaf() { return isLeaf; }
    public List<K> getKeys() { return keys; }
    public List<Integer> getChildren() { return children; }
    public List<V> getValues() { return values; }
    public int getMaxKeys() { return maxKeys; }
    public void setLeaf(boolean leaf) { this.isLeaf = leaf; }

    /** Insert or replace (upsert) inside a leaf, keeping keys sorted. */
    public void insertOrReplace(K key, V value) {
        if (!isLeaf) throw new IllegalStateException("insertOrReplace only for leaves");
        int pos = 0;
        while (pos < keys.size() && keys.get(pos).compareTo(key) < 0) pos++;
        if (pos < keys.size() && keys.get(pos).compareTo(key) == 0) {
            values.set(pos, value); // replace existing
        } else {
            keys.add(pos, key);
            values.add(pos, value);
        }
    }

    /** Overfull if we now have more than maxKeys. */
    public boolean isOverfull() {
        return keys.size() > maxKeys;
    }

    @Override
    public String toString() {
        if (isLeaf)
            return "LeafNode{keys=" + keys + ", values=" + values + "}";
        else
            return "InternalNode{keys=" + keys + ", children=" + children + "}";
    }
}
