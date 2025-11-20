package com.apacy.storagemanager.index;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BPlusTree<K extends Comparable<K>, V> {

    private final int branchingFactor;
    private Node<K,V> root;

    public class RangeIterator {

        private LeafNode<K,V> leaf;
        private int index;
        private final K upperBound;

        private boolean finished = false;

        RangeIterator(LeafNode<K,V> startLeaf, int startIndex, K ub) {
            this.leaf = startLeaf;
            this.index = startIndex;
            this.upperBound = ub;
        }

        public boolean hasNext() {
            if (finished || leaf == null) return false;

            if (index >= leaf.keys.size()) {
                leaf = leaf.next;
                index = 0;

                if (leaf == null) {
                    finished = true;
                    return false;
                }
            }

            if (leaf.keys.get(index).compareTo(upperBound) > 0) {
                finished = true;
                return false;
            }

            return true;
        }

        public V next() {
            if (!hasNext()) return null;

            V value = leaf.values.get(index);
            index++;
            return value;
        }
    }

    public BPlusTree(int branchingFactor) {
        if (branchingFactor < 3)
            throw new IllegalArgumentException("branchingFactor must be >= 3");
        this.branchingFactor = branchingFactor;
        this.root = new LeafNode<>(null);
    }

    public void insert(K key, V value) {
        LeafNode<K,V> leaf = findLeaf(key);
        leaf.insertInLeaf(key, value);

        if (leaf.isOverflow(branchingFactor)) {
            LeafNode<K,V> right = leaf.splitLeaf(branchingFactor);
            K firstKeyRight = right.getFirstKey();
            handleInsertParent(leaf, firstKeyRight, right);
        }
    }

    public void scanRange(K lb, K ub,
                          java.util.function.BiConsumer<K,V> consumer) {

        Node<K,V> C = root;

        while (!C.isLeaf()) {
            InternalNode<K,V> internal = (InternalNode<K,V>) C;

            int i = 0;
            while (i < internal.keys.size() &&
                   lb.compareTo(internal.keys.get(i)) > 0) {
                i++;
            }

            if (i == internal.keys.size()) {
                C = internal.children.get(i);
            } else {
                if (lb.compareTo(internal.keys.get(i)) == 0) {
                    C = internal.children.get(i + 1);
                } else {
                    C = internal.children.get(i);
                }
            }
        }

        LeafNode<K,V> leaf = (LeafNode<K,V>) C;

        int i = java.util.Collections.binarySearch(leaf.keys, lb);
        if (i < 0) i = -i - 1;
        if (i >= leaf.keys.size()) {
            i = leaf.keys.size();
        }

        boolean done = false;
        while (!done) {
            int n = leaf.keys.size();

            while (i < n) {
                K key = leaf.keys.get(i);
                if (key.compareTo(ub) <= 0) {
                    consumer.accept(key, leaf.values.get(i));
                    i++;
                } else {
                    done = true;
                    break;
                }
            }

            if (done) break;

            if (leaf.next != null) {
                leaf = leaf.next;
                i = 0;
            } else {
                done = true;
            }
        }
    }

    public static <K extends Comparable<K>, V> BPlusTree<K,V> fromRoot(int branchingFactor,
                                                                       Node<K,V> root) {
        BPlusTree<K,V> t = new BPlusTree<>(branchingFactor);
        t.root = root;
        return t;
    }

    private void handleInsertParent(Node<K,V> left,
                                    K keyToParent,
                                    Node<K,V> right) {
        if (left.parent == null) {
            InternalNode<K,V> newRoot = new InternalNode<>(null);
            newRoot.keys.add(keyToParent);
            newRoot.children.add(left);
            newRoot.children.add(right);
            left.parent = newRoot;
            right.parent = newRoot;
            this.root = newRoot;
            return;
        }

        InternalNode<K,V> parent = left.parent;
        parent.insertAfter(left, keyToParent, right);

        if (parent.isOverflow(branchingFactor)) {
            SplitInternal<K,V> split = parent.splitInternal(branchingFactor);
            handleInsertParent(parent, split.middleKey, split.rightNode);
        }
    }

    private LeafNode<K,V> findLeaf(K key) {
        Node<K,V> n = root;
        while (!n.isLeaf()) {
            InternalNode<K,V> internal = (InternalNode<K,V>) n;

            int pos = 0;
            while (pos < internal.keys.size() && key.compareTo(internal.keys.get(pos)) > 0)
                pos++;

            n = internal.children.get(pos);
        }
        return (LeafNode<K,V>) n;
    }


    public void delete(K key, V value) {
        LeafNode<K,V> leaf = findLeaf(key);
        boolean removed = leaf.deleteFromLeaf(key, value);
        if (!removed) return;

        if (leaf.isUnderflow(root == leaf, branchingFactor)) {
            handleUnderflow(leaf);
        }
    }

    private void handleUnderflow(Node<K,V> node) {

        if (node == root && node.isLeaf()) {
            if (node.keyCount() == 0) {
                root = new LeafNode<>(null);
            }
            return;
        }

        if (node == root && !node.isLeaf()) {
            InternalNode<K,V> r = (InternalNode<K,V>) root;
            if (r.children.size() == 1) {
                root = r.children.get(0);
                root.parent = null;
            }
            return;
        }


        InternalNode<K,V> parent = node.parent;
        int idx = parent.children.indexOf(node);

        Node<K,V> leftSibling  = (idx > 0) ? parent.children.get(idx - 1) : null;
        Node<K,V> rightSibling = (idx + 1 < parent.children.size())
                                 ? parent.children.get(idx + 1)
                                 : null;

        Node<K,V> sibling;
        int sepIndex;
        K separatorKey;

        if (leftSibling != null) {
            sibling   = leftSibling;
            sepIndex  = idx - 1;
        } else if (rightSibling != null) {
            sibling   = rightSibling;
            sepIndex  = idx;
        } else {
            return;
        }

        separatorKey = parent.keys.get(sepIndex);

        boolean merged = coalesceNodes(node, sibling, parent, sepIndex, separatorKey);

        if (merged) {
              if (parent == root) {
                  InternalNode<K,V> r = (InternalNode<K,V>) root;
                  if (!r.isLeaf() && r.children.size() == 1) {
                      root = r.children.get(0);
                      root.parent = null;
                  }
              } else if (parent.isUnderflow(false, branchingFactor)) {
                  handleUnderflow(parent);
              } 
        } else {
            redistributeNodes(node, sibling, parent, sepIndex);
        }
    }


    public List<V> rangeSearch(K lowerBound, K upperBound) {
        List<V> result = new ArrayList<>();

        LeafNode<K,V> leaf = findLeaf(lowerBound);

        int i = Collections.binarySearch(leaf.keys, lowerBound);
        if (i < 0) i = -i - 1;

        while (leaf != null) {
            while (i < leaf.keys.size()) {
                K k = leaf.keys.get(i);
                if (k.compareTo(upperBound) > 0) {
                    return result;
                }
                result.add(leaf.values.get(i));
                i++;
            }
            leaf = leaf.next;
            i = 0;
        }

        return result;
    }


    public V find(K key) {
        Node<K,V> C = root;

        while (!C.isLeaf()) {
            InternalNode<K,V> internal = (InternalNode<K,V>) C;

            int i = 0;
            while (i < internal.keys.size() &&
                   key.compareTo(internal.keys.get(i)) > 0) {
                i++;
            }

            if (i == internal.keys.size()) {
                C = internal.children.get(i);
            }
            else {
                if (key.compareTo(internal.keys.get(i)) == 0) {
                    C = internal.children.get(i + 1);
                } else {
                    C = internal.children.get(i);
                }
            }
        }

        LeafNode<K,V> leaf = (LeafNode<K,V>) C;

        int pos = Collections.binarySearch(leaf.keys, key);
        if (pos >= 0) {
            return leaf.values.get(pos);
        } else {
            return null;
        }
    }

    public List<V> findRange(K lb, K ub) {
        List<V> result = new ArrayList<>();

        Node<K,V> C = root;

        while (!C.isLeaf()) {
            InternalNode<K,V> internal = (InternalNode<K,V>) C;

            int i = 0;
            while (i < internal.keys.size() &&
                   lb.compareTo(internal.keys.get(i)) > 0) {
                i++;
            }

            if (i == internal.keys.size()) {
                C = internal.children.get(i);
            } else {
                if (lb.compareTo(internal.keys.get(i)) == 0) {
                    C = internal.children.get(i + 1);
                } else {
                    C = internal.children.get(i);
                }
            }
        }

        LeafNode<K,V> leaf = (LeafNode<K,V>) C;

        int i = Collections.binarySearch(leaf.keys, lb);
        if (i < 0) i = -i - 1;

        if (i >= leaf.keys.size()) {
            i = leaf.keys.size(); 
        }

        boolean done = false;

        while (!done) {
            int n = leaf.keys.size();

            while (i < n) {
                K key = leaf.keys.get(i);

                if (key.compareTo(ub) <= 0) {
                    result.add(leaf.values.get(i));
                    i++;
                } else {
                    done = true;
                    break;
                }
            }

            if (done) break;

            if (leaf.next != null) {
                leaf = leaf.next;
                i = 0;
            } else {
                done = true;
            }
        }

        System.out.println("[findRange] lb=" + lb + " ub=" + ub);
        System.out.println("[findRange] Keys in range:");
        for (V v : result) {
            System.out.println("  -> " + v);
        }
        System.out.println("[findRange] Total = " + result.size());

        return result;
    }


    public RangeIterator rangeIterator(K lb, K ub) {

        LeafNode<K,V> leaf = findLeaf(lb);

        int i = Collections.binarySearch(leaf.keys, lb);
        if (i < 0) i = -i - 1;

        if (i >= leaf.keys.size()) i = leaf.keys.size();

        return new RangeIterator(leaf, i, ub);
    }

    private boolean coalesceNodes(
            Node<K,V> N,
            Node<K,V> sibling,
            InternalNode<K,V> parent,
            int sepIndex,
            K separatorKey
    ) {

        boolean isLeaf = N.isLeaf();

        if (isLeaf) {
            LeafNode<K,V> L  = (LeafNode<K,V>) N;
            LeafNode<K,V> Lp = (LeafNode<K,V>) sibling;

            int total = L.keys.size() + Lp.keys.size();
            int nK = L.maxKeys(branchingFactor);

            if (total > nK) {
                return false;
            }
        } else {
            InternalNode<K,V> I  = (InternalNode<K,V>) N;
            InternalNode<K,V> Ip = (InternalNode<K,V>) sibling;

            int totalChildren = I.children.size() + Ip.children.size();
            if (totalChildren > branchingFactor) {
                return false; 
            }
        }

        Node<K,V> Left  = sibling;
        Node<K,V> Right = N;

        int idxL = parent.children.indexOf(Left);
        int idxR = parent.children.indexOf(Right);
        if (idxL > idxR) {
            Left = N;
            Right = sibling;
            sepIndex = Math.min(idxL, idxR);
            separatorKey = parent.keys.get(sepIndex);
        }

        if (Left.isLeaf()) {

            LeafNode<K,V> L  = (LeafNode<K,V>) Left;
            LeafNode<K,V> R  = (LeafNode<K,V>) Right;

            L.keys.addAll(R.keys);
            L.values.addAll(R.values);

            L.next = R.next;

        } else {

            InternalNode<K,V> L  = (InternalNode<K,V>) Left;
            InternalNode<K,V> R  = (InternalNode<K,V>) Right;

            L.keys.add(separatorKey);

            L.keys.addAll(R.keys);
            for (Node<K,V> child : R.children) {
                L.children.add(child);
                child.parent = L;
            }
        }

        parent.keys.remove(sepIndex);
        parent.children.remove(sepIndex + 1);

        return true;
    }

    private void redistributeNodes(
            Node<K,V> N,
            Node<K,V> sibling,
            InternalNode<K,V> parent,
            int sepIndex
    ) {

        boolean isLeaf = N.isLeaf();

        int idxN = parent.children.indexOf(N);
        int idxS = parent.children.indexOf(sibling);

        boolean siblingIsPredecessor = (idxS < idxN);

        if (isLeaf) {
            LeafNode<K,V> leafN = (LeafNode<K,V>) N;
            LeafNode<K,V> leafS = (LeafNode<K,V>) sibling;

            if (siblingIsPredecessor) {

                int lastIdx = leafS.keys.size() - 1;
                K borrowedKey = leafS.keys.remove(lastIdx);
                V borrowedVal = leafS.values.remove(lastIdx);

                leafN.keys.add(0, borrowedKey);
                leafN.values.add(0, borrowedVal);

                K newSep = leafS.keys.get(leafS.keys.size() - 1);
                parent.keys.set(sepIndex, newSep);

            } else {
                K borrowedKey = leafS.keys.remove(0);
                V borrowedVal = leafS.values.remove(0);

                leafN.keys.add(borrowedKey);
                leafN.values.add(borrowedVal);

                K newSep = leafS.keys.get(0);
                parent.keys.set(sepIndex, newSep);
            }

            return;
        }

        InternalNode<K,V> intN = (InternalNode<K,V>) N;
        InternalNode<K,V> intS = (InternalNode<K,V>) sibling;

        if (siblingIsPredecessor) {

            int lastKeyIdx = intS.keys.size() - 1;
            int lastChildIdx = intS.children.size() - 1;

            K keyFromSibling = intS.keys.remove(lastKeyIdx);
            Node<K,V> childFromSibling = intS.children.remove(lastChildIdx);

            intN.children.add(0, childFromSibling);
            childFromSibling.parent = intN;

            intN.keys.add(0, parent.keys.get(sepIndex)); // move K' down into N

            parent.keys.set(sepIndex, keyFromSibling);

        } else {
            Node<K,V> childFromSibling = intS.children.remove(0);
            K keyFromSibling = intS.keys.remove(0);

            intN.keys.add(parent.keys.get(sepIndex));
            intN.children.add(childFromSibling);
            childFromSibling.parent = intN;

            K newSep = intS.keys.get(0);
            parent.keys.set(sepIndex, newSep);
        }

    }


    public Node<K,V> getRoot() {
        return root;
    }


    // public static void main(String[] args) {

    //     System.out.println("========= B+TREE FULL RULE TEST START =========");

    //     int nV = 7; // branching factor
    //     BPlusTree<Integer, String> tree = new BPlusTree<>(nV);

    //     java.util.function.Consumer<Node<?,?>> printTree = node -> {
    //         System.out.println("\nCurrent Tree:");
    //         printNode(node, 0);
    //     };

    //     // ---------------------------
    //     // TEST INSERTION (I1–I26)
    //     // ---------------------------
    //     System.out.println("\n--- TEST INSERT ---");
    //     int[] keys = {5, 2, 8, 1, 3, 7, 6, 4, 9, 10, 11, 12};
    //     for (int k : keys) {
    //         tree.insert(k, "V" + k);
    //         System.out.println("Inserted " + k);
    //     }
    //     printTree.accept(tree.getRoot());

    //     // ---------------------------
    //     // TEST FIND (Q1–Q7)
    //     // ---------------------------
    //     System.out.println("\n--- TEST FIND ---");
    //     for (int k : keys) {
    //         String v = tree.find(k);
    //         if (v == null)
    //             System.out.println("FIND FAILED for key=" + k);
    //         else
    //             System.out.println("Find(" + k + ") = " + v);
    //     }

    //     // ---------------------------
    //     // TEST RANGE QUERY (R1–R12)
    //     // ---------------------------
    //     System.out.println("\n--- TEST RANGE ---");
    //     List<String> range = tree.findRange(3, 8);
    //     System.out.println("Range 3..8 = " + range);

    //     // ---------------------------
    //     // TEST RANGE ITERATOR (T1–T3)
    //     // ---------------------------
    //     System.out.println("\n--- TEST RANGE ITERATOR ---");
    //     BPlusTree<Integer,String>.RangeIterator it = tree.rangeIterator(3, 8);
    //     List<String> iterOut = new ArrayList<>();
    //     while (it.hasNext()) {
    //         iterOut.add(it.next());
    //     }
    //     System.out.println("Iterator 3..8 = " + iterOut);

    //     // ---------------------------
    //     // TEST DELETION WITH MERGE/REDISTRIBUTE & ROOT SHRINK (D1–D23)
    //     // ---------------------------
    //     System.out.println("\n--- TEST DELETE ---");
    //     int[] del = {1,2,3,4,5,6,7,8,9,10,11,12};
    //     for (int k : del) {
    //         System.out.println("Deleting " + k);
    //         tree.delete(k, "V" + k);
    //         printTree.accept(tree.getRoot());
    //     }

    //     // ---------------------------
    //     // TEST C1–C3: Composite Key
    //     // ---------------------------
    //     System.out.println("\n--- TEST COMPOSITE KEY INDEX (C1–C3) ---");

    //     BPlusTree<CompositeKey<Integer,Integer>, String> compTree =
    //             new BPlusTree<>(4);

    //     // Insert duplicates of attr “5” but unique pk
    //     compTree.insert(new CompositeKey<>(5, 1), "T1");
    //     compTree.insert(new CompositeKey<>(5, 2), "T2");
    //     compTree.insert(new CompositeKey<>(5, 3), "T3");
    //     compTree.insert(new CompositeKey<>(6, 1), "T4");
    //     compTree.insert(new CompositeKey<>(4, 1), "T5");

    //     printTree.accept(compTree.getRoot());

    //     // C2: equals-lookup using composite range
    //     CompositeKey<Integer,Integer> lb = new CompositeKey<>(5, Integer.MIN_VALUE);
    //     CompositeKey<Integer,Integer> ub = new CompositeKey<>(5, Integer.MAX_VALUE);

    //     List<String> dup5 = compTree.findRange(lb, ub);

    //     System.out.println("All attr=5 => " + dup5);

    //     System.out.println("\n========= B+TREE FULL RULE TEST END =========");
    // }

    @SuppressWarnings("unchecked")
    private static void printNode(Node<?,?> node, int depth) {
        String indent = "  ".repeat(depth);

        if (node.isLeaf()) {
            LeafNode<?,?> L = (LeafNode<?,?>) node;
            System.out.println(indent + "[Leaf] keys=" + L.keys);
        } else {
            InternalNode<?,?> I = (InternalNode<?,?>) node;
            System.out.println(indent + "[Internal] keys=" + I.keys);
            for (Node<?,?> c : I.children) {
                printNode(c, depth + 1);
            }
        }
    }
}
