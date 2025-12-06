package com.apacy.queryprocessor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.ParsedQuery;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.plan.ScanNode;
import com.apacy.common.enums.DataType;
import com.apacy.queryprocessor.mocks.MockStorageManager;

class QueryBinderTest {

    private QueryBinder queryBinder;
    private MockStorageManager mockSM;

    @BeforeEach
    void setUp() {
        mockSM = new MockStorageManager();
        
        // Setup Schema Dummy untuk Unit Test
        Schema studentSchema = new Schema(
            "students",
            "students.dat",
            List.of(
                new Column("nim", DataType.VARCHAR, 10),
                new Column("nama", DataType.VARCHAR, 100),
                new Column("ipk", DataType.FLOAT)
            ),
            Collections.emptyList()
        );
        mockSM.addSchema("students", studentSchema);

        queryBinder = new QueryBinder(mockSM);
    }

    /**
     * Unit Test 1: Binding Sukses
     * Memastikan kolom '*' di-expand menjadi kolom asli (nim, nama, ipk)
     * dan nama tabel valid.
     */
    @Test
    void testBind_Success_ExpandStar() {
        ParsedQuery rawQuery = new ParsedQuery(
            "SELECT",
            new ScanNode("students", "s"),
            List.of("students"),
            List.of("*"), // Input user: SELECT *
            Collections.emptyList(),
            null, null, null, false, false
        );

        ParsedQuery boundQuery = queryBinder.bind(rawQuery);

        // Verifikasi '*' berubah jadi list kolom lengkap
        List<String> cols = boundQuery.targetColumns();
        assertEquals(3, cols.size(), "Harus meng-expand 3 kolom dari schema students");
        assertTrue(cols.contains("students.nim"));
        assertTrue(cols.contains("students.nama"));
        assertTrue(cols.contains("students.ipk"));
    }

    /**
     * Unit Test 2: Binding Gagal (Tabel Tidak Ditemukan)
     * Memastikan QueryBinder melempar error jika tabel tidak ada di StorageManager.
     */
    @Test
    void testBind_Fail_TableNotFound() {
        ParsedQuery invalidQuery = new ParsedQuery(
            "SELECT",
            null,
            List.of("dosen_gaib"), // Tabel ngasal
            List.of("*"),
            Collections.emptyList(),
            null, null, null, false, false
        );

        Exception e = assertThrows(IllegalArgumentException.class, () -> {
            queryBinder.bind(invalidQuery);
        });

        assertTrue(e.getMessage().contains("Table 'dosen_gaib' does not exist"));
    }

    /**
     * Unit Test 3: Binding Gagal (Kolom Tidak Ditemukan)
     * Memastikan error validasi jika user meminta kolom yang salah.
     */
    @Test
    void testBind_Fail_ColumnNotFound() {
        ParsedQuery invalidColQuery = new ParsedQuery(
            "SELECT",
            null,
            List.of("students"),
            List.of("hobi"), // Kolom 'hobi' tidak ada di schema
            Collections.emptyList(),
            null, null, null, false, false
        );

        Exception e = assertThrows(IllegalArgumentException.class, () -> {
            queryBinder.bind(invalidColQuery);
        });

        assertTrue(e.getMessage().contains("Column 'hobi' not found"));
    }
}