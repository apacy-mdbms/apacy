package com.apacy.common.enums;

public enum IndexType {
    Hash(1),
    BPlusTree(2);

    private final int value;

    IndexType(int value) {
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
    public static IndexType fromValue(int value) {
        for (IndexType type : IndexType.values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Nilai tipe indeks tidak valid: " + value);
    }
}