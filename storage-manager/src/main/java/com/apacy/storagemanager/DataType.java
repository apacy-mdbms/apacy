package com.apacy.storagemanager;

public enum DataType {
    INTEGER(1),
    FLOAT(2),
    CHAR(3),
    VARCHAR(4);

    private final int value;

    DataType(int value) {
        this.value = value;
    }

    /**
     * Mengembalikan representasi integer untuk ditulis ke disk.
     */
    public int getValue() {
        return value;
    }

    /**
     * Helper statis untuk mengonversi int dari disk kembali ke enum.
     */
    public static DataType fromValue(int value) {
        for (DataType type : DataType.values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Nilai tipe data tidak valid: " + value);
    }
}