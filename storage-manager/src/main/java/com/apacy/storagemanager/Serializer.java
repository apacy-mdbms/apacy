package com.apacy.storagemanager;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.enums.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializer menangani konversi antara objek Row dan byte array,
 * DAN juga mengelola struktur "Slotted Page" di dalam blok.
 */
public class Serializer {
    private CatalogManager catalogManager;

    // --- Konstanta untuk Slotted Page Header ---
    // Header Blok: [Jumlah Slot (int: 4 byte)][Pointer Free Space (int: 4 byte)]
    private static final int HEADER_SLOT_COUNT_OFFSET = 0;
    private static final int HEADER_FREE_SPACE_OFFSET = 4;
    private static final int BLOCK_HEADER_SIZE = 8; // 4 + 4

    // Header Slot (Daftar Isi): [Offset Data (int: 4 byte)][Panjang Data (int: 4
    // byte)]
    private static final int SLOT_SIZE = 8; // 4 + 4
    private static final int SLOT_OFFSET_OFFSET = 0;
    private static final int SLOT_LENGTH_OFFSET = 4;
    private int lastSlotId = -1;

    public Serializer(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    /**
     * Metode UTAMA untuk MEMBACA (digunakan oleh Orang 2 & 4).
     * Mengambil seluruh blok 4KB dan mengurai "Slotted Page"-nya
     * untuk mengembalikan semua Row yang ada di dalamnya.
     */
    public List<Row> deserializeBlock(byte[] blockData, Schema schema) throws IOException {
        List<Row> rowsInBlock = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(blockData);

        // 1. Baca Header Blok
        int slotCount = buffer.getInt(HEADER_SLOT_COUNT_OFFSET);

        // 2. Iterasi melalui "Daftar Isi" (Slot Directory)
        for (int i = 0; i < slotCount; i++) {
            Row row = deserializeSlot(blockData, schema, i);
            if (row != null) {
                rowsInBlock.add(row);
            }

        }
        return rowsInBlock;
    }

    public Row deserializeSlot(byte[] blockData, Schema schema, int slotId) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(blockData);
        int slotCount = buffer.getInt(HEADER_SLOT_COUNT_OFFSET);
        if (slotId >= slotCount) {
            throw new IOException("Slot ID " + slotId + " di luar jangkauan (Total: " + slotCount + ")");
        }
        int slotOffset = BLOCK_HEADER_SIZE + (slotId * SLOT_SIZE);
        int dataOffset = buffer.getInt(slotOffset + SLOT_OFFSET_OFFSET);
        int dataLength = buffer.getInt(slotOffset + SLOT_LENGTH_OFFSET);

        if (dataOffset == 0 || dataLength == 0) {
            return null;
        }

        byte[] rowBytes = new byte[dataLength];
        System.arraycopy(blockData, dataOffset, rowBytes, 0, dataLength);

        return deserializeRow(rowBytes, schema);
    }

    /**
     * Metode UTAMA untuk MENULIS (digunakan oleh Orang 2).
     * Mengambil blok 4KB yang ada, lalu "mengepak" Row baru ke dalamnya.
     * Mengembalikan blok 4KB yang sudah diperbarui.
     * @param blockData Byte[] 4KB dari BlockManager
     * @param newRow Row baru yang ingin dimasukkan
     * @return Byte[] 4KB yang sudah diperbarui
     */
    public byte[] packRowToBlock(byte[] blockData, Row newRow, Schema schema) throws IOException {
        // 1. Ubah Row baru menjadi byte[]
        byte[] rowBytes = serializeRow(newRow, schema);
        int rowLength = rowBytes.length;

        ByteBuffer buffer = ByteBuffer.wrap(blockData);

        // 2. Baca Header Blok
        int slotCount = buffer.getInt(HEADER_SLOT_COUNT_OFFSET);
        int freeSpaceOffset = buffer.getInt(HEADER_FREE_SPACE_OFFSET); // Pointer ke awal spasi kosong

        // 3. Cari slot yang ditandai sebagai "deleted"
        for (int slotId = 0; slotId < slotCount; slotId++) {
            int slotOffset = BLOCK_HEADER_SIZE + (slotId * SLOT_SIZE);
            int dataOffset = buffer.getInt(slotOffset + SLOT_OFFSET_OFFSET);
            int dataLength = buffer.getInt(slotOffset + SLOT_LENGTH_OFFSET);

            // Jika slot ini kosong (deleted), gunakan kembali slot ini
            if (dataOffset == 0 && dataLength == 0) {
                // Tulis data Row baru di lokasi free space
                int newDataOffset = freeSpaceOffset - rowLength;
                if (newDataOffset < (BLOCK_HEADER_SIZE + (slotCount * SLOT_SIZE))) {
                    throw new IOException("Blok penuh, tidak cukup spasi untuk Row baru.");
                }

                System.arraycopy(rowBytes, 0, blockData, newDataOffset, rowLength);

                // Perbarui slot yang dihapus dengan data baru
                buffer.putInt(slotOffset + SLOT_OFFSET_OFFSET, newDataOffset);
                buffer.putInt(slotOffset + SLOT_LENGTH_OFFSET, rowLength);

                // Perbarui header blok
                buffer.putInt(HEADER_FREE_SPACE_OFFSET, newDataOffset); // Geser pointer spasi kosong
                this.lastSlotId = slotId;

                return blockData;
            }
        }

        // 4. Jika tidak ada slot yang dihapus, tambahkan slot baru
        int spaceNeeded = rowLength + SLOT_SIZE;
        int nextSlotOffset = BLOCK_HEADER_SIZE + (slotCount * SLOT_SIZE);

        if (freeSpaceOffset - nextSlotOffset < spaceNeeded) {
            throw new IOException("Blok penuh, tidak cukup spasi untuk Row baru.");
        }

        // Tulis data Row baru di lokasi free space
        int newDataOffset = freeSpaceOffset - rowLength;
        System.arraycopy(rowBytes, 0, blockData, newDataOffset, rowLength);

        // Tulis slot baru di daftar isi
        buffer.putInt(nextSlotOffset + SLOT_OFFSET_OFFSET, newDataOffset);
        buffer.putInt(nextSlotOffset + SLOT_LENGTH_OFFSET, rowLength);

        // Perbarui header blok
        buffer.putInt(HEADER_SLOT_COUNT_OFFSET, slotCount + 1); // Tambah jumlah slot
        buffer.putInt(HEADER_FREE_SPACE_OFFSET, newDataOffset); // Geser pointer spasi kosong
        this.lastSlotId = slotCount;

        return blockData;
    }

    /**
     * Inisialisasi blok 4KB yang masih kosong.
     * Menyiapkan header Slotted Page.
     */
    public byte[] initializeNewBlock() {
        byte[] blockData = new byte[BlockManager.DEFAULT_BLOCK_SIZE]; // Asumsi 4096
        ByteBuffer buffer = ByteBuffer.wrap(blockData);

        // Jumlah slot = 0
        buffer.putInt(HEADER_SLOT_COUNT_OFFSET, 0);
        // Free space dimulai dari akhir blok
        buffer.putInt(HEADER_FREE_SPACE_OFFSET, BlockManager.DEFAULT_BLOCK_SIZE);

        return blockData;
    }

    /**
     * (HELPER) Serialize SATU Row ke byte[] (Ukuran Variabel).
     */
    private byte[] serializeRow(Row row, Schema schema) throws IOException {
        // 1. Hitung ukuran pasti
        int totalSize = estimateSize(row, schema);

        // 2. Alokasikan ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        Map<String, Object> data = row.data();

        // 3. Tulis data ke buffer
        for (Column col : schema.columns()) {
            Object value = data.get(col.name());

            switch (col.type()) {
                case INTEGER: // INTEGER
                    buffer.putInt((Integer) value);
                    break;
                case FLOAT: // FLOAT
                    buffer.putFloat((Float) value);
                    break;
                case CHAR: // CHAR
                case VARCHAR: // VARCHAR
                    byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                    // 4. Tulis prefix panjang (4 byte int)
                    buffer.putInt(strBytes.length);
                    // 5. Tulis data string
                    buffer.put(strBytes);
                    break;
                default:
                    throw new IOException("Tipe data tidak didukung: " + col.type());
            }
        }
        return buffer.array();
    }

    /**
     * (HELPER) Deserialize byte array menjadi SATU Row object.
     * TODO: Implementasikan logika konversi tipe data yang sebenarnya di sini.
     */
    public Row deserializeRow(byte[] data, Schema schema) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Map<String, Object> rowData = new HashMap<>();

        for (Column col : schema.columns()) {
            switch (col.type()) {
                case INTEGER: // INTEGER
                    rowData.put(col.name(), buffer.getInt());
                    break;
                case FLOAT: // FLOAT
                    rowData.put(col.name(), buffer.getFloat());
                    break;
                case CHAR: // CHAR
                case VARCHAR: // VARCHAR
                    // 1. Baca prefix panjang (4 byte int)
                    int strLength = buffer.getInt();
                    // 2. Alokasikan byte[] seukuran panjang itu
                    byte[] strBytes = new byte[strLength];
                    // 3. Baca data string ke byte[]
                    buffer.get(strBytes);
                    rowData.put(col.name(), new String(strBytes, StandardCharsets.UTF_8));
                    break;
                default:
                    throw new IOException("Tipe data tidak didukung: " + col.type());
            }
        }
        return new Row(rowData);
    }

    /**
     * Calculate the estimated size of a serialized Row.
     * TODO: Implement size estimation logic
     */
    public int estimateSize(Row row, Schema schema) {
        int totalSize = 0;
        Map<String, Object> data = row.data();

        for (Column col : schema.columns()) {
            Object value = data.get(col.name());

            switch (col.type()) {
                case INTEGER: // INTEGER
                    totalSize += Integer.BYTES; // 4 byte
                    break;
                case FLOAT: // FLOAT
                    totalSize += Float.BYTES; // 4 byte
                    break;
                case CHAR: // VARCHAR
                case VARCHAR: // TEXT
                    // 4 byte (untuk prefix panjang int) + N byte (data UTF-8)
                    totalSize += Integer.BYTES;
                    totalSize += ((String) value).getBytes(StandardCharsets.UTF_8).length;
                    break;
            }
        }
        return totalSize;
    }

    public int getLastPackedSlotId() {
        return this.lastSlotId;
    }

    public Row readRowAtSlot(byte[] blockData, Schema schema, int slotId) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(blockData);

        int slotCount = buffer.getInt(HEADER_SLOT_COUNT_OFFSET);
        if (slotId < 0 || slotId >= slotCount) {
            return null;
        }

        int slotOffset = BLOCK_HEADER_SIZE + (slotId * SLOT_SIZE);

        int dataOffset = buffer.getInt(slotOffset + SLOT_OFFSET_OFFSET);
        int dataLength = buffer.getInt(slotOffset + SLOT_LENGTH_OFFSET);

        if (dataOffset == 0 || dataLength == 0) {
            return null;
        }

        byte[] rowBytes = new byte[dataLength];
        System.arraycopy(blockData, dataOffset, rowBytes, 0, dataLength);

        return deserializeRow(rowBytes, schema);
    }
    
    /**
     * Get the number of slots in a block helper
     * @param blockData
     * @return
     */
    public int getSlotCount(byte[] blockData) {
        return ByteBuffer.wrap(blockData).getInt(HEADER_SLOT_COUNT_OFFSET);
    }

    /**
     * Delete a slot from a block
     * @param blockData
     * @param slotId
     * @return
     */
    public boolean deleteSlot(byte[] blockData, int slotId) {
        ByteBuffer buffer = ByteBuffer.wrap(blockData);
        int slotCount = buffer.getInt(HEADER_SLOT_COUNT_OFFSET);
        if (slotId < 0 || slotId >= slotCount) {
            return false;
        }

        int slotOffset = BLOCK_HEADER_SIZE + (slotId * SLOT_SIZE);
        int dataOffset = buffer.getInt(slotOffset + SLOT_OFFSET_OFFSET);
        int dataLength = buffer.getInt(slotOffset + SLOT_LENGTH_OFFSET);

        if (dataOffset == 0 && dataLength == 0) {
            return false;
        }

        buffer.putInt(slotOffset + SLOT_OFFSET_OFFSET, 0);
        buffer.putInt(slotOffset + SLOT_LENGTH_OFFSET, 0);
        return true;
    }
}
