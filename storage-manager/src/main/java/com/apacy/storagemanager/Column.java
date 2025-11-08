package com.apacy.storagemanager;

public class Column {
    private String name;
    private DataType type;
    private int length;

    public Column(String name, DataType type, int length) {
        this.name = name;
        this.type = type;
        this.length = (type == DataType.CHAR || type == DataType.VARCHAR) ? length : 
                      (type == DataType.INTEGER || type == DataType.FLOAT) ? 4 : 0;
    }
    
    public Column(String name, DataType type) {
        this(name, type, 0); 
    }

    public DataType getType() { return type; }
    public int getLength() { 
        if (type == DataType.INTEGER || type == DataType.FLOAT) return 4;
        return length; 
    }
    public String getName() { return name; }
    
    public int getFixedSize() {
        switch (type) {
            case INTEGER: return 4;
            case FLOAT:   return 4;
            case CHAR:
            case VARCHAR: return length;
            default:      return 0;
        }
    }
}