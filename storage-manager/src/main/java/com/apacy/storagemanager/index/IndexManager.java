package com.apacy.storagemanager.index;

import com.apacy.storagemanager.BlockManager;
import com.apacy.storagemanager.Serializer;
import com.apacy.common.enums.DataType;
import com.apacy.common.dto.Column;
import com.apacy.common.dto.Schema;

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
        Map<String, Map<String, IIndex<?, ?>>> colMap = indexes.get(table);
        if (colMap == null) return null;

        Map<String, IIndex<?, ?>> typeMap = colMap.get(column);
        if (typeMap == null) return null;

        return typeMap.get(type);
    }

    public void loadAll() {
        for (var tableEntry : indexes.values())
            for (var colEntry : tableEntry.values())
                for (var idx : colEntry.values())
                    idx.loadFromFile();
    }

    public void flushAll() {
        for (var tableEntry : indexes.values())
            for (var colEntry : tableEntry.values())
                for (var idx : colEntry.values())
                    idx.writeToFile();
    }

    public void drop(String table, String column, String type) {
        IIndex<?, ?> index = get(table, column, type);
        if (index != null) {
            index.remove();
            indexes.get(table).get(column).remove(type);
        }
    }

}
