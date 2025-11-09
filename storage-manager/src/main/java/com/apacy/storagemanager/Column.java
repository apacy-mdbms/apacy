package com.apacy.storagemanager;

public class Column {
    private String name;
    private DataType type;
    private int length; // 0 untuk INT/FLOAT/TEXT, >0 untuk CHAR/VARCHAR

    public Column(String name, DataType type, int length) {
        this.name = name;
        this.type = type;
        // Simpan 'length' apa adanya (misal 50 untuk VARCHAR(50))
        this.length = length;
    }
    
    public Column(String name, DataType type) {
        // Panggil konstruktor utama dengan length 0
        this(name, type, 0); 
    }

    public DataType getType() { return type; }
    public String getName() { return name; }

    public int getLength() { 
        return length; 
    }
}