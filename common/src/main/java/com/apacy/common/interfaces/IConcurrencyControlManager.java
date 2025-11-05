package com.apacy.common.interfaces;

import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ExecutionResult;

/**
 * Kontrak untuk: Concurrency Control Manager
 * Tugas: Mengatur izin baca/tulis untuk transaksi.
 */
public interface IConcurrencyControlManager {
    
    int beginTransaction(); // returns transaction_id

    void logObject(Row object, int transactionId);

    /**
     * Memvalidasi apakah suatu aksi diizinkan pada objek.
     * @param objectId ID unik objek (misal: "TABLE::employee" atau "ROW::employee::123")
     * @param transactionId ID transaksi yang meminta
     * @param action Aksi yang diminta (READ atau WRITE)
     * @return Objek Response (allowed=true/false)
     */
    Response validateObject(String objectId, int transactionId, Action action);

    void endTransaction(int transactionId, boolean commit);
}