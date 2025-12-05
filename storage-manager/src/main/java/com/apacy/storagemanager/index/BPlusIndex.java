package com.apacy.storagemanager.index;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.enums.DataType;
import com.apacy.storagemanager.BlockManager;
import com.apacy.storagemanager.CatalogManager;
import com.apacy.storagemanager.Serializer;

import java.io.IOException;
import java.util.*;

public class BPlusIndex<K extends Comparable<K>, V> implements IIndex<K, V> {

    private final String tableName;
    private final String columnName;
    private final int order;
    private final String indexFile;
    private final BlockManager blockManager;
    private final Serializer serializer;

    private BPlusTree<K, V> tree;

    public BPlusIndex(String tableName, String columnName) {
        this(tableName, columnName, 100);
    }

    public BPlusIndex(String tableName, String columnName, int order) {
        this(tableName,
                columnName,
                order,
                tableName + "_" + columnName + "_bplusindex", 
                null,
                null);
    }

    /**
     * Full constructor mirroring HashIndex style.
     *
     * @param tableName    base table name
     * @param columnName   indexed column
     * @param order        B+Tree branching factor (nV)
     * @param indexFile    index data file name
     * @param blockManager storage block manager (may be null → in-memory only)
     * @param serializer   serializer (may be null → in-memory only)
     */
    public BPlusIndex(String tableName,
            String columnName,
            int order,
            String indexFile,
            BlockManager blockManager,
            Serializer serializer) {

        this.tableName = tableName;
        this.columnName = columnName;
        this.order = order;
        this.indexFile = indexFile;
        this.blockManager = blockManager;
        this.serializer = serializer;
        this.tree = new BPlusTree<>(order);
    }

    /**
     * Build the on-disk schema for this B+ index.
     *
     * Each row = one node.
     *
     * Columns:
     * nodeId : INTEGER
     * isLeaf : INTEGER (0/1)
     * parentId : INTEGER (-1 = root)
     * nextLeafId : INTEGER (for leaf-level linked list; -1 if none)
     * keyCount : INTEGER (number of valid keys)
     * childCount : INTEGER (for internal nodes: number of valid children)
     *
     * For i in [0, maxKeys-1] where maxKeys = order - 1:
     * key_i : same DataType as base column
     * val_i : INTEGER (address / pointer; only used for leaves)
     *
     * For j in [0, order-1]:
     * child_j : INTEGER (child nodeId; only used for internal nodes)
     */
    private Schema buildSchema(CatalogManager catalogManager) {
        Schema baseSchema = catalogManager.getSchema(tableName);
        if (baseSchema == null) {
            throw new IllegalStateException("Base table '" + tableName + "' not found in CatalogManager.");
        }

        Column baseColumn = baseSchema.getColumnByName(columnName);
        if (baseColumn == null) {
            throw new IllegalStateException("Column '" + columnName + "' not found in table '" + tableName + "'");
        }

        List<Column> cols = new ArrayList<>();

        cols.add(new Column("nodeId", DataType.INTEGER));
        cols.add(new Column("isLeaf", DataType.INTEGER));
        cols.add(new Column("parentId", DataType.INTEGER));
        cols.add(new Column("nextLeafId", DataType.INTEGER));
        cols.add(new Column("keyCount", DataType.INTEGER));
        cols.add(new Column("childCount", DataType.INTEGER));

        int maxKeys = order - 1;

        for (int i = 0; i < maxKeys; i++) {
            cols.add(new Column("key_" + i, baseColumn.type(), baseColumn.length()));
        }

        for (int i = 0; i < maxKeys; i++) {
            cols.add(new Column("val_" + i, DataType.INTEGER));
        }

        for (int i = 0; i < order; i++) {
            cols.add(new Column("child_" + i, DataType.INTEGER));
        }

        String indexTableName = tableName + "_" + columnName + "_bplusindex";

        return new Schema(
                indexTableName,
                indexFile,
                cols,
                List.of());
    }

    private static Object defaultKeyValue(DataType type) {
        return switch (type) {
            case INTEGER -> 0;
            case FLOAT -> 0.0f;
            case CHAR,
                    VARCHAR ->
                "";
        };
    }

    @Override
    public void remove() {
        this.tree = new BPlusTree<>(order);

        if (blockManager == null || serializer == null) {
            return;
        }

        try {
            byte[] blk = serializer.initializeNewBlock();
            blockManager.writeBlock(indexFile, 0, blk);
            blockManager.flush();
        } catch (IOException ex) {
            System.err.println("BPlusIndex.remove error: " + ex.getMessage());
        }
    }

    @Override
    public void loadFromFile(CatalogManager catalogManager) {
        if (blockManager == null || serializer == null) {
            return;
        }

        try {
            Schema schema = buildSchema(catalogManager);
            long blockCount = blockManager.getBlockCount(indexFile);

            if (blockCount == 0) {
                this.tree = new BPlusTree<>(order);
                return;
            }

            Map<Integer, NodeRecord<K, V>> recordMap = new HashMap<>();

            for (long bn = 0; bn < blockCount; bn++) {
                byte[] blk = blockManager.readBlock(indexFile, bn);
                List<Row> rows = serializer.deserializeBlock(blk, schema);

                for (Row r : rows) {
                    Map<String, Object> data = r.data();

                    int nodeId = (int) data.get("nodeId");
                    int isLeafFlag = (int) data.get("isLeaf");
                    int parentId = (int) data.get("parentId");
                    int nextLeafId = (int) data.get("nextLeafId");
                    int keyCount = (int) data.get("keyCount");
                    int childCount = (int) data.get("childCount");

                    boolean isLeaf = (isLeafFlag == 1);

                    NodeRecord<K, V> rec = new NodeRecord<>();
                    rec.nodeId = nodeId;
                    rec.parentId = parentId;
                    rec.nextLeafId = nextLeafId;
                    rec.isLeaf = isLeaf;
                    rec.childIds = new ArrayList<>();

                    int maxKeys = order - 1;

                    if (isLeaf) {
                        LeafNode<K, V> leaf = new LeafNode<>(null);

                        for (int i = 0; i < keyCount; i++) {
                            Object attr = data.get("key_" + i);

                            @SuppressWarnings("unchecked")
                            V rid = (V) data.get("val_" + i);

                            @SuppressWarnings({ "rawtypes", "unchecked" })
                            CompositeKey ck = new CompositeKey((Comparable) attr, (Comparable) rid);

                            leaf.keys.add((K) ck);
                            leaf.values.add(rid);
                        }

                        rec.node = leaf;
                    } else {
                        InternalNode<K, V> internal = new InternalNode<>(null);

                        for (int i = 0; i < keyCount; i++) {
                            Object attr = data.get("key_" + i);

                            @SuppressWarnings({ "rawtypes", "unchecked" })
                            CompositeKey ck = new CompositeKey((Comparable) attr, (Comparable) Integer.valueOf(0));

                            internal.keys.add((K) ck);
                        }

                        for (int c = 0; c < childCount; c++) {
                            int cid = (int) data.get("child_" + c);
                            rec.childIds.add(cid);
                        }

                        rec.node = internal;
                    }

                    recordMap.put(nodeId, rec);
                }
            }

            if (recordMap.isEmpty()) {
                this.tree = new BPlusTree<>(order);
                return;
            }

            Node<K, V> root = null;

            for (NodeRecord<K, V> rec : recordMap.values()) {
                if (rec.parentId == -1) {
                    root = rec.node;
                    break;
                }
            }

            if (root == null) {
                this.tree = new BPlusTree<>(order);
                return;
            }

            for (NodeRecord<K, V> rec : recordMap.values()) {
                if (!rec.isLeaf) {
                    InternalNode<K, V> internal = (InternalNode<K, V>) rec.node;
                    for (Integer cid : rec.childIds) {
                        NodeRecord<K, V> childRec = recordMap.get(cid);
                        if (childRec == null)
                            continue;

                        internal.children.add(childRec.node);
                        childRec.node.parent = internal;
                    }
                } else {
                    LeafNode<K, V> leaf = (LeafNode<K, V>) rec.node;
                    if (rec.nextLeafId >= 0) {
                        NodeRecord<K, V> nextRec = recordMap.get(rec.nextLeafId);
                        if (nextRec != null && nextRec.isLeaf) {
                            leaf.next = (LeafNode<K, V>) nextRec.node;
                        }
                    }
                }

                if (rec.parentId >= 0) {
                    NodeRecord<K, V> parentRec = recordMap.get(rec.parentId);
                    if (parentRec != null && parentRec.node instanceof InternalNode<?, ?>) {
                        @SuppressWarnings("unchecked")
                        InternalNode<K, V> p = (InternalNode<K, V>) parentRec.node;
                        rec.node.parent = p;
                    }
                }
            }

            this.tree = BPlusTree.fromRoot(order, root);

        } catch (Exception e) {
            System.err.println("BPlusIndex.loadFromFile error: " + e.getMessage());
            this.tree = new BPlusTree<>(order);
        }
    }

    private Object unwrapCompositeKey(Object key) {
        if (key instanceof CompositeKey<?, ?> ck) {
            return ck.getAttr();
        }
        return key;
    }

    @Override
    public void writeToFile(CatalogManager catalogManager) {
        if (blockManager == null || serializer == null) {
            return;
        }

        try {
            Schema schema = buildSchema(catalogManager);
            Schema baseSchema = catalogManager.getSchema(tableName);
            if (baseSchema == null) {
                throw new IllegalStateException("Base table '" + tableName + "' not found in CatalogManager.");
            }
            Column baseColumn = baseSchema.getColumnByName(columnName);
            if (baseColumn == null) {
                throw new IllegalStateException("Column '" + columnName + "' not found in table '" + tableName + "'");
            }
            DataType keyType = baseColumn.type();
            Object defaultKey = defaultKeyValue(keyType);

            if (tree == null || tree.getRoot() == null) {
                tree = new BPlusTree<>(order);
            }

            Node<K, V> rootNode = tree.getRoot();
            List<Node<K, V>> nodes = new ArrayList<>();
            collectNodes(rootNode, nodes);

            Map<Node<K, V>, Integer> idMap = new HashMap<>();
            for (int i = 0; i < nodes.size(); i++) {
                idMap.put(nodes.get(i), i);
            }

            int maxKeys = order - 1;
            List<Row> rows = new ArrayList<>();

            for (Node<K, V> n : nodes) {
                Map<String, Object> data = new HashMap<>();
                int nodeId = idMap.get(n);

                data.put("nodeId", nodeId);
                data.put("isLeaf", n.isLeaf() ? 1 : 0);
                data.put("parentId", (n.parent == null) ? -1 : idMap.get(n.parent));
                data.put("nextLeafId", -1);
                data.put("keyCount", n.keyCount());

                if (n.isLeaf()) {
                    LeafNode<K, V> leaf = (LeafNode<K, V>) n;

                    if (leaf.next != null) {
                        Integer nextId = idMap.get(leaf.next);
                        data.put("nextLeafId", nextId != null ? nextId : -1);
                    }

                    for (int i = 0; i < maxKeys; i++) {
                        if (i < leaf.keys.size()) {
                            Object keyObj = leaf.keys.get(i);
                            data.put("key_" + i, unwrapCompositeKey(keyObj));
                            data.put("val_" + i, leaf.values.get(i));
                        } else {
                            data.put("key_" + i, defaultKey);
                            data.put("val_" + i, 0);
                        }
                    }

                    data.put("childCount", 0);
                    for (int c = 0; c < order; c++) {
                        data.put("child_" + c, -1);
                    }

                } else {
                    InternalNode<K, V> internal = (InternalNode<K, V>) n;

                    for (int i = 0; i < maxKeys; i++) {
                        if (i < internal.keys.size()) {
                            Object keyObj = internal.keys.get(i);
                            data.put("key_" + i, unwrapCompositeKey(keyObj));
                        } else {
                            data.put("key_" + i, defaultKey);
                        }
                        data.put("val_" + i, 0);
                    }

                    data.put("childCount", internal.children.size());
                    for (int c = 0; c < order; c++) {
                        if (c < internal.children.size()) {
                            data.put("child_" + c, idMap.get(internal.children.get(c)));
                        } else {
                            data.put("child_" + c, -1);
                        }
                    }
                }

                rows.add(new Row(data));
            }

            byte[] block = serializer.initializeNewBlock();
            long blockNo = 0;

            for (Row r : rows) {
                try {
                    block = serializer.packRowToBlock(block, r, schema);
                } catch (IOException full) {
                    blockManager.writeBlock(indexFile, blockNo, block);
                    block = serializer.initializeNewBlock();
                    block = serializer.packRowToBlock(block, r, schema);
                    blockNo = blockManager.appendBlock(indexFile, block);
                }
            }

            blockManager.writeBlock(indexFile, blockNo, block);
            blockManager.flush();

        } catch (Exception e) {
            System.err.println("BPlusIndex.writeToFile error: " + e.getMessage());
        }
    }

    private Comparable minValueFor(Comparable pk) {
        if (pk instanceof Integer)
            return Integer.MIN_VALUE;
        if (pk instanceof Long)
            return Long.MIN_VALUE;
        if (pk instanceof Short)
            return Short.MIN_VALUE;
        if (pk instanceof Byte)
            return Byte.MIN_VALUE;
        if (pk instanceof Float)
            return -Float.MAX_VALUE;
        if (pk instanceof Double)
            return -Double.MAX_VALUE;
        if (pk instanceof String)
            return "";
        throw new IllegalArgumentException("Unsupported PK type: " + pk.getClass());
    }

    private Comparable maxValueFor(Comparable pk) {
        if (pk instanceof Integer)
            return Integer.MAX_VALUE;
        if (pk instanceof Long)
            return Long.MAX_VALUE;
        if (pk instanceof Short)
            return Short.MAX_VALUE;
        if (pk instanceof Byte)
            return Byte.MAX_VALUE;
        if (pk instanceof Float)
            return Float.MAX_VALUE;
        if (pk instanceof Double)
            return Double.MAX_VALUE;
        if (pk instanceof String)
            return Character.toString(Character.MAX_VALUE);
        throw new IllegalArgumentException("Unsupported PK type: " + pk.getClass());
    }

    @Override
    public List<V> getAddress(K key) {
        if (tree == null)
            return Collections.emptyList();

        if (key instanceof CompositeKey<?, ?> ck) {
            return tree.findRange(key, true, key, true);
        }

        Comparable attr = (Comparable) key;

        Comparable minPk = minValueFor((Comparable)0);
        Comparable maxPk = maxValueFor((Comparable)0);

        @SuppressWarnings("unchecked")
        K lb = (K) new CompositeKey(attr, minPk);
        @SuppressWarnings("unchecked")
        K ub = (K) new CompositeKey(attr, maxPk);

        return tree.findRange(lb, true, ub, true);
    }

    @Override
    public List<V> getAddresses(K minKey, boolean minInclusive, K maxKey, boolean maxInclusive) {
        if (tree == null) {
            return Collections.emptyList();
        }

        K start = minKey;
        if (start != null && !(start instanceof CompositeKey<?, ?>)) {
            Comparable val = (Comparable) start;
            Comparable pkBound = minInclusive ? minValueFor((Comparable)0) : maxValueFor((Comparable)0);
            @SuppressWarnings("unchecked")
            K lower = (K) new CompositeKey(val, pkBound); 
            start = lower;
        }

        K end = maxKey;
        if (end != null && !(end instanceof CompositeKey<?, ?>)) {
            Comparable val = (Comparable) end;
            Comparable pkBound = maxInclusive ? maxValueFor((Comparable)0) : minValueFor((Comparable)0);
            @SuppressWarnings("unchecked")
            K upper = (K) new CompositeKey(val, pkBound);
            end = upper;
        }

        return tree.findRange(start, minInclusive, end, maxInclusive);
    }

    @Override
    public void insertData(K key, V address) {
        if (tree == null) {
            tree = new BPlusTree<>(order);
        }

        CompositeKey ck = new CompositeKey(
                (Comparable) key,
                (Comparable) address);

        tree.insert((K) ck, address);
    }

    @Override
    public void deleteData(K key, V address) {
        if (tree == null)
            return;

        K deleteKey = key;
        if (!(key instanceof CompositeKey<?, ?>)) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            CompositeKey ck = new CompositeKey((Comparable) key, (Comparable) address);
            deleteKey = (K) ck;
        }

        tree.delete(deleteKey, address);
    }

    public Map<K, List<V>> rangeQuery(K start, K end) {
        if (tree == null)
            return Collections.emptyMap();

        Map<K, List<V>> result = new LinkedHashMap<>();

        tree.scanRange(start, end, (k, v) -> {
            result.computeIfAbsent(k, kk -> new ArrayList<>()).add(v);
        });

        return result;
    }

    private void collectNodes(Node<K, V> node, List<Node<K, V>> out) {
        if (node == null)
            return;
        out.add(node);
        if (!node.isLeaf()) {
            InternalNode<K, V> internal = (InternalNode<K, V>) node;
            for (Node<K, V> child : internal.children) {
                collectNodes(child, out);
            }
        }
    }

    private static class NodeRecord<KK extends Comparable<KK>, VV> {
        int nodeId;
        int parentId;
        int nextLeafId;
        boolean isLeaf;
        Node<KK, VV> node;
        List<Integer> childIds;
    }
}
