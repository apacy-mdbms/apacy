package com.apacy.queryoptimizer.parser;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals; // Import parser utama
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ParsedQuery;
import com.apacy.queryoptimizer.QueryParser;

class InsertParserTest {

    private QueryParser parser; // Kita uji melalui QueryParser utama

    @BeforeEach
    void setUp() {
        parser = new QueryParser();
    }

    @Test
    void testParseInsert_Success() throws Exception {
        String query = "INSERT INTO users (id, name, age) VALUES ('user1', 'Budi', 25);";
        ParsedQuery pq = parser.parse(query);

        assertNotNull(pq);
        assertEquals("INSERT", pq.queryType());
        assertEquals("users", pq.targetTables().get(0));
        assertEquals(List.of("id", "name", "age"), pq.targetColumns());
        
        // Cek field 'values' yang baru
        assertEquals(List.of("'user1'", "'Budi'", "25"), pq.values());
    }

    @Test
    void testParseInsert_Fail_MismatchCount() {
        String query = "INSERT INTO users (id, name) VALUES ('user1')"; // Kolom 2, Nilai 1
        
        // Panggil parser.parse() di dalam assertThrows
        Exception exception = assertThrows(Exception.class, () -> {
            parser.parse(query);
        });
        
        // Cek bahwa pesan error-nya benar
        assertTrue(exception.getMessage().contains("Jumlah kolom tidak cocok"));
    }
    
    // Test baru untuk method validate()
    @Test
    void testValidateInsert_Success() {
        String query = "INSERT INTO users (id, name, age) VALUES ('user1', 'Budi', 25);";
        assertTrue(parser.validate(query));
    }

    @Test
    void testValidateInsert_Fail() {
        String query = "INSERT INTO users (id, name) VALUES ('user1');"; // Jumlah tidak cocok
        assertFalse(parser.validate(query));
    }
}