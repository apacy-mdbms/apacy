package com.apacy.failurerecoverymanager;

import com.apacy.common.dto.Row;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LogDataParserTest {

    @Test
    void testToMapReturnsNullForNullOrDash() {
        assertNull(LogDataParser.toMap(null));
        assertNull(LogDataParser.toMap("-"));
    }

    @Test
    void testToMapWithRow() {
        Row row = new Row(Map.of("id", 1, "name", "Alice"));
        Map<String, Object> result = LogDataParser.toMap(row);

        assertNotNull(result);
        assertEquals("Alice", result.get("name"));
    }

    @Test
    void testToMapWithMapInstance() {
        Map<String, Object> payload = Map.of("salary", 1000);
        Map<String, Object> result = LogDataParser.toMap(payload);

        assertSame(payload, result, "Existing map should be returned as-is");
    }

    @Test
    void testToMapWithSerializedRowString() {
        String serialized = "Row{data={id=2, name=Bob}}";
        Map<String, Object> result = LogDataParser.toMap(serialized);

        assertNotNull(result);
        assertEquals("Bob", result.get("name"));
        assertEquals("2", result.get("id"));
    }

    @Test
    void testToMapWithMalformedString() {
        Map<String, Object> result = LogDataParser.toMap("Row{data={invalid}}");
        assertNotNull(result, "Malformed string should return empty map instead of null");
        assertTrue(result.isEmpty());
    }
}

