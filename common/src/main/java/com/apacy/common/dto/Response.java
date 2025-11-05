package com.apacy.common.dto;

import java.util.Map;

/**
 * Data Transfer Object for generic responses.
 * 
 * @param statusCode HTTP-style status code (200 for success, 400/500 for errors)
 * @param message Response message
 * @param data Additional response data
 * @param timestamp Response timestamp in milliseconds
 * @param requestId Unique request identifier for tracing
 */
public record Response(
    int statusCode,
    String message,
    Map<String, Object> data,
    long timestamp,
    String requestId
) {
    public Response {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (statusCode < 100 || statusCode >= 600) {
            throw new IllegalArgumentException("Invalid status code: " + statusCode);
        }
    }
    
    public boolean isSuccess() {
        return statusCode >= 200 && statusCode < 300;
    }
}