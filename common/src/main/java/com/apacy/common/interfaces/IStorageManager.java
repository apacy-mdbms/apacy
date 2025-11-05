package com.apacy.common.interfaces;

import com.apacy.common.dto.*;
import java.util.List;

/**
 * Kontrak untuk: Storage Manager
 * Tugas: Mengelola baca/tulis/hapus data fisik di disk.
 */
public interface IStorageManager {
    
    List<Row> readBlock(DataRetrieval dataRetrieval);

    int writeBlock(DataWrite dataWrite); // returns affected rows

    int deleteBlock(DataDeletion dataDeletion); // returns affected rows

    void setIndex(String table, String column, String indexType);

    Statistic getStats();
}