package com.apacy.queryprocessor.mocks;

import com.apacy.common.dto.*;
import com.apacy.common.interfaces.IStorageManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockStorageManager implements IStorageManager {

    @Override
    public List<Row> readBlock(DataRetrieval dataRetrieval) {
        
        List<Row> dummy = new ArrayList<>();

        Row row1 = new Row(Map.of(
            "id", 1,
            "name", "Naufarrel",
            "salary", 20000
        ));

        Row row2 = new Row(Map.of(
            "id", 2,
            "name", "Weka",
            "salary", 30000
        ));

        Row row3 = new Row(Map.of(
            "id", 3,
            "name", "Kinan",
            "salary", 40000
        ));

        Row row4 = new Row(Map.of(
            "id", 4,
            "name", "Farrel",
            "salary", 50000
        ));

        Row row5 = new Row(Map.of(
            "id", 5,
            "name", "Bayu",
            "salary", 60000
        ));
        
        dummy.add(row1);
        dummy.add(row2);
        dummy.add(row3);
        dummy.add(row4);
        dummy.add(row5);

        return dummy;
    }

    @Override
    public int writeBlock(DataWrite dataWrite) {
        throw new UnsupportedOperationException("writeBlock not implemented yet");
    }

    @Override
    public int deleteBlock(DataDeletion dataDeletion) {
        throw new UnsupportedOperationException("deleteBlock not implemented yet");
    }

    @Override
    public void setIndex(String table, String column, String indexType) {
        throw new UnsupportedOperationException("setIndex not implemented yet");
    }

    @Override
    public Statistic getStats() {
        throw new UnsupportedOperationException("getStats not implemented yet");
    }
}