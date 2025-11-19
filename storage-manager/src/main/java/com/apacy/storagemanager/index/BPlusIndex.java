package com.apacy.storagemanager.index;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import com.apacy.storagemanager.CatalogManager;

public class BPlusIndex<K extends Comparable<K>, V> implements IIndex<K, V> {

    private final String tableName;
    private final String columnName;
    private final int order;
    private BPlusTree<K,V> tree;

    public BPlusIndex(String tableName, String columnName) {
        this(tableName, columnName, 100);
    }

    public BPlusIndex(String tableName, String columnName, int order) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.order = order;
    }


    @Override
    public void remove() {
    }

    @Override
    public void loadFromFile(CatalogManager catalogManager) {
    }

    @Override
    public void writeToFile(CatalogManager catalogManager) {
    }

    @Override
    public List<V> getAddress(K key) {
      return Collections.emptyList(); 
    }

    @Override
    public void insertData(K key, V address) {
    }

    @Override
    public void deleteData(K key, V address) {
    }

    // Example range query (extra method)
    public Map<K, List<V>> rangeQuery(K start, K end) {
      return Collections.emptyMap(); 
    }
}
