package com.apacy.storagemanager;

import java.util.List;

public class Row {
    private Schema schema;
    private List<Object> values;

    public Row(Schema schema, List<Object> values) {
        if (schema.getColumnCount() != values.size()) {
            throw new IllegalArgumentException("Jumlah kolom skema (" + schema.getColumnCount() 
                + ") dan nilai (" + values.size() + ") tidak cocok!");
        }
        this.schema = schema;
        this.values = values;
    }
    public Schema getSchema() { return schema; }
    public List<Object> getValues() { return values; }
    public Object getValue(int index) { return values.get(index); }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Row[\n");
        for (int i = 0; i < schema.getColumnCount(); i++) {
            sb.append("  ").append(schema.getColumn(i).getName()).append(" (").append(schema.getColumn(i).getType()).append(")")
              .append(": ").append(values.get(i)).append("\n");
        }
        return sb.toString();
    }
}