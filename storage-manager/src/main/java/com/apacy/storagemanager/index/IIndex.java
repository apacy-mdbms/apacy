package com.apacy.storagemanager.index;

import com.apacy.storagemanager.CatalogManager;
import java.util.List;

public interface IIndex<K, V> {
    // construction bikin .dat file utk index, writeToFile, set tableName dan columnName, dan loadFromFile
    
    // hapus file index
    void remove();
    
    // load indexing dr .dat ke memory (path ditentukan dri kolom+tablename+indextype+.dat)
    void loadFromFile(CatalogManager catalogManager);

    // ubah data yang ada di .dat berdasarkan yang ada di memory
    void writeToFile(CatalogManager catalogManager);
    
    // dapet addresses berdasarkan key
    List<V> getAddress(K key);

    List<V> getAddresses(K minKey, boolean minInclusive, K maxKey, boolean maxInclusive);
    
    // insert data baru ke memory dulu
    void insertData(K key, V address);
    
    // delete data yang ada di memory
    void deleteData(K key, V address);
}
