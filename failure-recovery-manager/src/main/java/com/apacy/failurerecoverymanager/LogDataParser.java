package com.apacy.failurerecoverymanager;

import com.apacy.common.dto.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility helper to convert serialized log payloads (String / Row / Map)
 * into a mutable {@link Map} representation that can be consumed by the
 * storage manager for redo/undo operations.
 */
final class LogDataParser {

    private LogDataParser() {
        // utility
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> toMap(Object data) {
        if (data == null || "-".equals(data)) {
            return null;
        }

        if (data instanceof Row row) {
            return row.data();
        }

        if (data instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }

        if (data instanceof String raw) {
            Map<String, Object> parsed = new HashMap<>();
            String cleaned = raw
                .replace("Row{data=", "")
                .replaceAll("[\\{\\}\\[\\]]", "")
                .trim();

            if (cleaned.isEmpty()) {
                return parsed;
            }

            String[] keyValues = cleaned.split(",");
            for (String keyValue : keyValues) {
                String[] parts = keyValue.split("=");
                if (parts.length >= 2) {
                    parsed.put(parts[0].trim(), parts[1].trim());
                }
            }
            return parsed;
        }

        return null;
    }
}

