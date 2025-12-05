package com.apacy.failurerecoverymanager;

import java.util.HashMap;
import java.util.Map;

import com.apacy.common.dto.Row;

final class LogDataParser {

    private LogDataParser() {
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
                    String key = parts[0].trim();
                    String value = parts[1].trim();
                    // Don't add null or "null" string values
                    if (value != null && !value.equalsIgnoreCase("null") && !value.isEmpty()) {
                        parsed.put(key, value);
                    }
                }
            }
            return parsed;
        }

        return null;
    }
}
