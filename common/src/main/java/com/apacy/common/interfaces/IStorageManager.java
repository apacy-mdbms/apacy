package com.apacy.common.interfaces;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.Statistic;

/**
 * Kontrak untuk: Storage Manager
 * Tugas: Mengelola baca/tulis/hapus data fisik di disk.
 */
public interface IStorageManager {
    
    List<Row> readBlock(DataRetrieval dataRetrieval);

    int writeBlock(DataWrite dataWrite); // returns affected rows

    int deleteBlock(DataDeletion dataDeletion); // returns affected rows

    void setIndex(String table, String column, String indexType);

    Map<String, Statistic> getAllStats(); // returns statistic of each tables. String == Table name

    void createTable(Schema schema) throws IOException;

    // void dropTable(String tableName, boolean isCascading) throws IOException;
}