package com.apacy.storagemanager.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BPlusTree<K extends Comparable<K>, V> {

    private final int order;                // e.g., 4
    private final int maxKeys;              // order-1
    private final List<Node<K,V>> nodes;    // pool of nodes
    private int rootIndex;

    public BPlusTree(int order) {
        if (order < 3) throw new IllegalArgumentException("order must be >= 3");
        this.order = order;
        this.maxKeys = order - 1;
        this.nodes = new ArrayList<>();
        Node<K,V> root = new Node<>(maxKeys, true);
        nodes.add(root);
        this.rootIndex = 0;
    }

    public BPlusTree() { this(4); }

    // ---------- Public API ----------

    public V search(K key) {
        return searchRecursive(rootIndex, key);
    }

    public void insert(K key, V value) {
        int rightOfRoot = insertRecursive(rootIndex, key, value);
        if (rightOfRoot != -1) {
            // Root was split: create a new root with [oldRoot, rightOfRoot]
            Node<K,V> newRoot = new Node<>(maxKeys, false);
            newRoot.getChildren().add(rootIndex);
            newRoot.getChildren().add(rightOfRoot);
            // Parent guide key = minimum key of right subtree
            K upKey = getFirstKey(rightOfRoot);
            newRoot.getKeys().add(upKey);
            nodes.add(newRoot);
            rootIndex = nodes.size() - 1;
        }
    }

    public void printTree() {
        System.out.println("B+ Tree (order=" + order + ")");
        for (int i = 0; i < nodes.size(); i++)
            System.out.println("Node " + i + ": " + nodes.get(i));
        System.out.println("Root index: " + rootIndex);
    }

    public List<Node<K,V>> getNodes() { return nodes; }
    public int getRootIndex() { return rootIndex; }

    // ---------- Core logic ----------

    private V searchRecursive(int nodeIndex, K key) {
        Node<K,V> node = nodes.get(nodeIndex);
        if (node.isLeaf()) {
            List<K> keys = node.getKeys();
            List<V> vals = node.getValues();
            for (int i = 0; i < keys.size(); i++)
                if (keys.get(i).compareTo(key) == 0) return vals.get(i);
            return null;
        } else {
            int childPos = findChildIndex(node, key);
            return searchRecursive(node.getChildren().get(childPos), key);
        }
    }

    /**
     * Inserts into subtree rooted at nodeIndex.
     * Returns -1 if no split, else returns the index of the newly created RIGHT sibling.
     * Parent must insert (at position childPos+1) that returned index and a guide key = min(right subtree).
     */
    private int insertRecursive(int nodeIndex, K key, V value) {
        Node<K,V> node = nodes.get(nodeIndex);

        if (node.isLeaf()) {
            node.insertOrReplace(key, value);
            if (!node.isOverfull()) return -1;
            return splitLeaf(nodeIndex);
        } else {
            int childPos = findChildIndex(node, key);
            int childIndex = node.getChildren().get(childPos);
            int rightSibling = insertRecursive(childIndex, key, value);

            // If child split, stitch in the new right sibling and rebuild guide keys
            if (rightSibling != -1) {
                node.getChildren().add(childPos + 1, rightSibling);
                // add guide key for the new right subtree = its minimum key
                K upKey = getFirstKey(rightSibling);
                node.getKeys().add(childPos, upKey);
            }

            // Regardless of split or not, a new smallest key might have appeared in a left child.
            // Rebuild guide keys from children to stay consistent.
            rebuildInternalKeys(nodeIndex);

            if (!node.isOverfull()) return -1;
            return splitInternal(nodeIndex);
        }
    }

    /**
     * Find child index using guide keys:
     * keys[i] == first key of children[i+1].
     * We pick the first key > target; that's child index 'pos'.
     */
    private int findChildIndex(Node<K,V> node, K key) {
        int pos = 0;
        List<K> keys = node.getKeys();
        while (pos < keys.size() && key.compareTo(keys.get(pos)) >= 0) pos++;
        return pos; // child at 'pos'
    }

    /** Recompute guide keys so that keys[i] = firstKey(children[i+1]). */
    private void rebuildInternalKeys(int nodeIndex) {
        Node<K,V> node = nodes.get(nodeIndex);
        if (node.isLeaf()) return;
        List<Integer> ch = node.getChildren();
        List<K> keys = node.getKeys();
        keys.clear();
        for (int i = 1; i < ch.size(); i++) {
            keys.add(getFirstKey(ch.get(i)));
        }
    }

    /** Get minimum key in subtree rooted at nodeIndex. */
    private K getFirstKey(int nodeIndex) {
        int idx = nodeIndex;
        while (!nodes.get(idx).isLeaf()) {
            idx = nodes.get(idx).getChildren().get(0);
        }
        Node<K,V> leaf = nodes.get(idx);
        return leaf.getKeys().get(0);
    }

    /** Split a leaf node into [left=nodeIndex, right=newIndex]; return right index. */
    private int splitLeaf(int nodeIndex) {
        Node<K,V> left = nodes.get(nodeIndex);
        Node<K,V> right = new Node<>(maxKeys, true);

        int n = left.getKeys().size();             // n == maxKeys+1
        

        int mid = (n + 1) / 2; // bias right
        // move keys >= splitKey to right
        for (int i = mid; i < n; i++) {
            right.getKeys().add(left.getKeys().get(i));
            right.getValues().add(left.getValues().get(i));
        }
        left.getKeys().subList(mid, n).clear();
        left.getValues().subList(mid, n).clear();

        // ensure parent guide key comes from *right leaf’s first key*
        nodes.add(right);
        return nodes.size() - 1;
    }

    /** Split an internal node; return index of the new RIGHT node. */
    private int splitInternal(int nodeIndex) {
        Node<K,V> left = nodes.get(nodeIndex);
        Node<K,V> right = new Node<>(maxKeys, false);

        // Split by children; keys will be rebuilt from children for both sides.
        int totalChildren = left.getChildren().size(); // == left.keys.size()+1 == maxKeys+2
        int midChildren = totalChildren / 2;           // balanced: left gets midChildren, right gets rest

        // Move right half children to 'right'
        for (int i = midChildren; i < totalChildren; i++) {
            right.getChildren().add(left.getChildren().get(i));
        }
        // Keep left half children
        left.getChildren().subList(midChildren, totalChildren).clear();

        // Rebuild guide keys based on new children
        rebuildInternalKeys(nodeIndex);
        nodes.add(right);
        int rightIndex = nodes.size() - 1;
        rebuildInternalKeys(rightIndex);

        return rightIndex;
    }

    // ---------- Demo main from your test ----------

    public static void main(String[] args) {
        System.out.println("=== B+ Tree Manual Test ===\n");

        BPlusTree<Integer, String> tree = new BPlusTree<>(4);
        int[] keys = {10, 20, 5, 6, 12, 30, 7, 17};
        char v = 'A';
        for (int k : keys) tree.insert(k, String.valueOf(v++));
        System.out.println("Inserted initial keys.");
        tree.printTree();

        System.out.println("\n=== Basic Search ===");
        System.out.println("Search(6): " + tree.search(6));
        System.out.println("Search(17): " + tree.search(17));
        System.out.println("Search(25): " + tree.search(25));

        System.out.println("\n=== Large Insert & Lookup ===");
        for (int i = 1; i <= 100; i++) tree.insert(i, String.format("v%03d", i));
        boolean allGood = true;
        for (int i = 1; i <= 100; i++) {
            String expected = String.format("v%03d", i);
            String got = tree.search(i);
            if (!expected.equals(got)) {
                allGood = false;
                System.out.println("Mismatch at key " + i + ": expected " + expected + " but got " + got);
                break;
            }
        }
        System.out.println(allGood ? "All 1..100 verified OK" : "Some lookup mismatches!");
        System.out.println("Search(-1): " + tree.search(-1));
        System.out.println("Search(101): " + tree.search(101));

        System.out.println("\n=== Structure Invariants After Bulk Insert ===");
        for (int i = 1; i <= 200; i++) tree.insert(i, "x" + i);

        int order = 4;
        int nodeCount = 0;
        boolean structureOk = true;
        for (Node<Integer, String> n : tree.getNodes()) {
            nodeCount++;
            if (n.getKeys().size() > (order - 1)) {
                structureOk = false;
                System.out.println("Node has too many keys: " + n);
            }
            if (!n.isLeaf()) {
                if (n.getChildren().size() != n.getKeys().size() + 1) {
                    structureOk = false;
                    System.out.println("Internal node children != keys+1: " + n);
                }
            } else {
                if (n.getValues() == null) {
                    structureOk = false;
                    System.out.println("Leaf has null values: " + n);
                }
                if (n.getKeys().size() != n.getValues().size()) {
                    structureOk = false;
                    System.out.println("Leaf key/value size mismatch: " + n);
                }
            }
        }
        System.out.println(structureOk ? "All structure invariants OK (" + nodeCount + " nodes)." : "Structure issues found!");

        System.out.println("\n=== Random Lookups ===");
        Random rnd = new Random(42);
        for (int i = 0; i < 150; i++) {
            int k = rnd.nextInt(1000);
            tree.insert(k, "v" + k); // upsert if re-hit
        }
        int found = 0, checked = 0;
        for (int i = 0; i < 50; i++) {
            int k = rnd.nextInt(1000);
            String got = tree.search(k);
            checked++;
            if (got != null) {
                found++;
                if (!got.equals("v" + k))
                    System.out.println("Inconsistent value for key " + k + ": got " + got);
            }
        }
        System.out.println("Random lookup test done. Found " + found + " / " + checked + " existing keys.");

        System.out.println("\n=== B+ Tree Structure (visual) ===");
        tree.printTree();
    }
}
