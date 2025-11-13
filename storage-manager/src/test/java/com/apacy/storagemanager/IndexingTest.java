package com.apacy.storagemanager;

import com.apacy.common.enums.DataType;
import com.apacy.storagemanager.index.HashIndex;
import com.apacy.storagemanager.index.IndexManager;
import com.apacy.storagemanager.index.IIndex;
import com.apacy.storagemanager.BlockManager;
import com.apacy.storagemanager.Serializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class IndexingTest {

    private IndexManager manager;
    private BlockManager blockManager;

    @BeforeEach
    void setup() throws Exception {
        manager = new IndexManager();

        // Create temp directory for test index files
        Path tempDir = Files.createTempDirectory("idx_test_");

        // Initialize real BlockManager + Serializer
        blockManager = new BlockManager(tempDir.toString());
    }

    // Helper to create a correct HashIndex using REAL constructor
    private HashIndex<Integer, Integer> newIndex(String name) {
        return new HashIndex<>(
                "testTable",           // tableName
                "testColumn",          // columnName
                name + ".idx",         // indexFileName
                DataType.INTEGER,      // keyType
                DataType.INTEGER,      // valueType
                blockManager,          // blockManager
        );
    }

    @Test
    void testRegisterAndGet() {
        HashIndex<Integer, Integer> index = newIndex("users_id_hash");

        manager.register("users", "id", "HASH", index);

        IIndex<?, ?> retrieved = manager.get("users", "id", "HASH");

        assertNotNull(retrieved);
        assertSame(index, retrieved);
    }

    @Test
    void testInsertAndGetFromHashIndex() {
        HashIndex<Integer, Integer> index = newIndex("product_sku_hash");
        manager.register("product", "sku", "HASH", index);

        @SuppressWarnings("unchecked")
        IIndex<Integer, Integer> idx = (IIndex<Integer, Integer>)
                manager.get("product", "sku", "HASH");

        idx.insertData(10, 1000);
        idx.insertData(20, 2000);

        assertEquals(1000, idx.getAddress(10).get(0));
        assertEquals(2000, idx.getAddress(20).get(0));
        assertTrue(idx.getAddress(99).isEmpty());
    }

    @Test
    void testDropIndex() {
        HashIndex<Integer, Integer> index = newIndex("orders_id_hash");

        manager.register("orders", "id", "HASH", index);
        assertNotNull(manager.get("orders", "id", "HASH"));

        manager.drop("orders", "id", "HASH");

        // after drop, get() must return null
        assertNull(manager.get("orders", "id", "HASH"));
    }

    @Test
    void testLoadAndFlush() {
        HashIndex<Integer, Integer> idx1 = newIndex("t1_c1_hash");
        HashIndex<Integer, Integer> idx2 = newIndex("t2_c2_hash");

        manager.register("t1", "c1", "HASH", idx1);
        manager.register("t2", "c2", "HASH", idx2);

        // These should NOT throw any exceptions
        manager.loadAll();
        manager.flushAll();
    }
}
