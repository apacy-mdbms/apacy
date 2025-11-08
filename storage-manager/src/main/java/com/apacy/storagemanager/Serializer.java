package com.apacy.storagemanager;

// import com.apacy.common.dto.Row;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Serializer handles conversion between Row objects and byte arrays for storage.
 * TODO: Implement serialization/deserialization with proper type handling
 */
public class Serializer {
    
    /**
     * Serialize a Row object to a byte array.
     * TODO: Implement serialization with proper type encoding and error handling
     */
    public byte[] serialize(Row row) throws IOException {
        // TODO: Implement row serialization logic
        Schema schema = row.getSchema();
        int rowSize = schema.getFixedRowSize();
        
        ByteBuffer buffer = ByteBuffer.allocate(rowSize);

        for (int i = 0; i < schema.getColumnCount(); i++) {
            Column col = schema.getColumn(i);
            Object value = row.getValue(i);

            switch (col.getType()) {
                case INTEGER:
                    buffer.putInt((Integer) value);
                    break;
                case FLOAT:
                    buffer.putFloat((Float) value);
                    break;
                case CHAR:
                case VARCHAR:
                    String strValue = (String) value;
                    byte[] strBytes = strValue.getBytes(StandardCharsets.UTF_8);
                    byte[] fixedBytes = new byte[col.getLength()];
                    System.arraycopy(strBytes, 0, fixedBytes, 0, Math.min(strBytes.length, col.getLength()));
                    buffer.put(fixedBytes);
                    break;
                default:
                    throw new IOException("Tipe data tidak didukung: " + col.getType());
            }
        }
        return buffer.array();
    }
    
    /**
     * Deserialize a byte array to a Row object.
     * TODO: Implement deserialization with proper type decoding and validation
     */
    public Row deserialize(byte[] data, Schema schema) throws IOException {
        // TODO: Implement row deserialization logic

        ByteBuffer buffer = ByteBuffer.wrap(data);
        List<Object> values = new ArrayList<>();

        for (Column col : schema.getColumns()) {
            switch (col.getType()) {
                case INTEGER:
                    values.add(buffer.getInt());
                    break;
                case FLOAT:
                    values.add(buffer.getFloat());
                    break;
                case CHAR:
                case VARCHAR:
                    byte[] fixedBytes = new byte[col.getLength()];
                    buffer.get(fixedBytes);
                    String strValue = new String(fixedBytes, StandardCharsets.UTF_8).trim();
                    values.add(strValue);
                    break;
                default:
                    throw new IOException("Tipe data tidak didukung: " + col.getType());
            }
        }
        return new Row(schema, values);
    }
    
    /**
     * Calculate the estimated size of a serialized Row.
     * TODO: Implement size estimation for memory management
     */
    public int estimateSize(Row row) {
        // TODO: Implement size estimation logic
        throw new UnsupportedOperationException("estimateSize not implemented yet");
    }
}