package com.apacy.storagemanager.index;

import com.apacy.storagemanager.CatalogManager;
import java.util.Map;
import java.util.HashMap;

public class IndexManager {
    private final Map<String, Map<String, Map<String, IIndex<?, ?>>>> indexes = new HashMap<>();

    public void register(String table, String column, String type, IIndex<?, ?> index) {
        indexes
            .computeIfAbsent(table, t -> new HashMap<>())
            .computeIfAbsent(column, c -> new HashMap<>())
            .put(type, index);
    }

    public IIndex<?, ?> get(String table, String column, String type) {
        var colMap = indexes.get(table);
        if (colMap == null) return null;
        var typeMap = colMap.get(column);
        if (typeMap == null) return null;
        return typeMap.get(type);
    }

    public void loadAll(CatalogManager catalogManager) {
        for (var tableEntry : indexes.values())
            for (var colEntry : tableEntry.values())
                for (var idx : colEntry.values())
                    idx.loadFromFile(catalogManager);
    }

    public void flushAll(CatalogManager catalogManager) {
        for (var tableEntry : indexes.values())
            for (var colEntry : tableEntry.values())
                for (var idx : colEntry.values())
                    idx.writeToFile(catalogManager);
    }

    public void drop(String table, String column, String type) {
        var index = get(table, column, type);
        if (index != null) {
            index.remove();
            indexes.get(table).get(column).remove(type);
        }
    }
}

