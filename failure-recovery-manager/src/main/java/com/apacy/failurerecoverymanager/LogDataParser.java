package com.apacy.failurerecoverymanager;

import java.util.HashMap;
import java.util.Map;

import com.apacy.common.dto.Row;

final class LogDataParser {
    private LogDataParser() {}

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(Object data) {
        if (data == null || "-".equals(data)) return null;
        if (data instanceof Row row) return row.data();
        if (data instanceof Map<?, ?> map) return (Map<String, Object>) map;

        if (data instanceof String raw) {
            Map<String, Object> parsed = new HashMap<>();
            String cleaned = raw.replaceAll("^Row[\\[\\{]data=", "");
            if (cleaned.endsWith("}") || cleaned.endsWith("]")) {
                cleaned = cleaned.substring(0, cleaned.length() - 1);
            }
            cleaned = cleaned.replaceAll("[\\{\\}\\[\\]]", "").trim();
            
            if (cleaned.isEmpty()) return parsed;

            String[] keyValues = cleaned.split(",");
            for (String keyValue : keyValues) {
                String[] parts = keyValue.split("=", 2);
                if (parts.length >= 2) {
                    String key = parts[0].trim();
                    String value = parts[1].trim();
                    if (!key.isEmpty() && value != null && !value.equalsIgnoreCase("null") && !value.isEmpty()) {
                        parsed.put(key, value);
                    }
                }
            }
            return parsed;
        }
        return null;
    }
}