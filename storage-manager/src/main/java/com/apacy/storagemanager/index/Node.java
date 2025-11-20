package com.apacy.storagemanager.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class Node<K extends Comparable<K>, V> {

    protected final List<K> keys = new ArrayList<>();

    protected InternalNode<K,V> parent;

    Node(InternalNode<K,V> parent) {
        this.parent = parent;
    }

    abstract boolean isLeaf();

    int keyCount() {
        return keys.size();
    }

    K getFirstKey() {
        return keys.get(0);
    }

    abstract int minKeys(int branchingFactor);

    int maxKeys(int branchingFactor) {
        return branchingFactor - 1;
    }

    boolean isOverflow(int branchingFactor) {
        return keyCount() > maxKeys(branchingFactor);
    }

    boolean isUnderflow(boolean isRoot, int branchingFactor) {
        if (isRoot) return false;
        return keyCount() < minKeys(branchingFactor);
    }
}

final class LeafNode<K extends Comparable<K>, V> extends Node<K,V> {

    final List<V> values = new ArrayList<>();

    LeafNode<K,V> next;

    LeafNode(InternalNode<K,V> parent) {
        super(parent);
    }

    @Override
    boolean isLeaf() {
        return true;
    }

    @Override
    int minKeys(int branchingFactor) {
        int nK = maxKeys(branchingFactor);
        return (int) Math.ceil(nK / 2.0);
    }

    void insertInLeaf(K key, V value) {
        int pos = Collections.binarySearch(keys, key);
        if (pos >= 0) {
            // composite key: exact match -> replace value
            values.set(pos, value);
        } else {
            pos = -pos - 1;
            keys.add(pos, key);
            values.add(pos, value);
        }
    }

    boolean deleteFromLeaf(K key, V value) {
        int pos = Collections.binarySearch(keys, key);
        if (pos < 0) return false;

        int i = pos;
        while (i < keys.size() && keys.get(i).compareTo(key) == 0) {
            if ((values.get(i) == null && value == null) ||
                (values.get(i) != null && values.get(i).equals(value))) {
                keys.remove(i);
                values.remove(i);
                return true;
            }
            i++;
        }
        return false;
    }

    LeafNode<K,V> splitLeaf(int branchingFactor) {
        int totalKeys = keyCount();
        int splitIndex = (totalKeys + 1) / 2;

        LeafNode<K,V> right = new LeafNode<>(parent);

        right.keys.addAll(keys.subList(splitIndex, totalKeys));
        right.values.addAll(values.subList(splitIndex, totalKeys));

        keys.subList(splitIndex, totalKeys).clear();
        values.subList(splitIndex, totalKeys).clear();

        right.next = this.next;
        this.next = right;

        return right;
    }
}

final class InternalNode<K extends Comparable<K>, V> extends Node<K,V> {

    /** Children P1..Pm; must satisfy children.size() == keys.size() + 1. */
    final List<Node<K,V>> children = new ArrayList<>();

    InternalNode(InternalNode<K,V> parent) {
        super(parent);
    }

    @Override
    boolean isLeaf() {
        return false;
    }

    @Override
    int minKeys(int branchingFactor) {
        int nV = branchingFactor;
        int minChildren = (int) Math.ceil(nV / 2.0);
        return minChildren - 1;
    }

    void insertAfter(Node<K,V> leftChild, K key, Node<K,V> newChild) {
        int index = children.indexOf(leftChild);
        if (index < 0) throw new IllegalArgumentException("Left child not found");
        keys.add(index, key);
        children.add(index + 1, newChild);
        newChild.parent = this;
    }

    SplitInternal<K,V> splitInternal(int branchingFactor) {
        int totalChildren = children.size();
        int midChildIndex = totalChildren / 2;
        int midKeyIndex = midChildIndex - 1;

        K middleKey = keys.get(midKeyIndex);

        InternalNode<K,V> right = new InternalNode<>(parent);

        right.keys.addAll(keys.subList(midKeyIndex + 1, keys.size()));
        right.children.addAll(children.subList(midChildIndex, children.size()));

        for (Node<K,V> child : right.children) {
            child.parent = right;
        }

        keys.subList(midKeyIndex, keys.size()).clear();
        children.subList(midChildIndex, children.size()).clear();

        return new SplitInternal<>(middleKey, right);
    }
}

final class SplitInternal<K extends Comparable<K>, V> {
    final K middleKey;
    final InternalNode<K,V> rightNode;

    SplitInternal(K middleKey, InternalNode<K,V> rightNode) {
        this.middleKey = middleKey;
        this.rightNode = rightNode;
    }
}

