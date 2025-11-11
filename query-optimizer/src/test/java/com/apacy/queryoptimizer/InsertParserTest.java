package com.apacy.queryoptimizer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals; // Import parser utama
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apacy.common.dto.ParsedQuery;

class InsertParserTest {

    private QueryParser parser; // Kita uji melalui QueryParser utama

    @BeforeEach
    void setUp() {
        parser = new QueryParser();
    }

    @Test
    void testParseInsert_Success() {
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
        
        Exception exception = assertThrows(RuntimeException.class, () -> {
            parser.parse(query);
        });
        
        assertEquals("Jumlah kolom tidak cocok dengan jumlah nilai untuk INSERT.", exception.getMessage());
    }

    @Test
    void testParseInsert_Fail_MissingValuesKeyword() {
        String query = "INSERT INTO users (id, name) ('user1', 'Budi')";
        
        Exception exception = assertThrows(RuntimeException.class, () -> {
            parser.parse(query);
        });
        
        // Ini error dari AbstractParser.consume()
        assertEquals("Expected VALUES but found LPARENTHESIS", exception.getMessage());
    }
    
    @Test
    void testParseInsert_Fail_WrongValueType() {
        String query = "INSERT INTO users (id, name) VALUES (user1, 'Budi')"; // user1 bukan literal
        
        Exception exception = assertThrows(RuntimeException.class, () -> {
            parser.parse(query);
        });

        assertTrue(exception.getMessage().contains("Diharapkan STRING_LITERAL atau NUMBER_LITERAL"));
    }
}