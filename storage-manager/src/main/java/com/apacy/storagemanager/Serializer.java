package com.apacy.storagemanager;

import com.apacy.common.dto.Row;
import java.io.IOException;
import java.nio.ByteBuffer; // Menggunakan ByteBuffer sangat membantu
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serializer menangani konversi antara objek Row dan byte array, 
 * DAN juga mengelola struktur "Slotted Page" di dalam blok.
 */
public class Serializer {

    // --- Konstanta untuk Slotted Page Header ---
    // Header Blok: [Jumlah Slot (int: 4 byte)][Pointer Free Space (int: 4 byte)]
    private static final int HEADER_SLOT_COUNT_OFFSET = 0;
    private static final int HEADER_FREE_SPACE_OFFSET = 4;
    private static final int BLOCK_HEADER_SIZE = 8; // 4 + 4

    // Header Slot (Daftar Isi): [Offset Data (int: 4 byte)][Panjang Data (int: 4 byte)]
    private static final int SLOT_SIZE = 8; // 4 + 4
    private static final int SLOT_OFFSET_OFFSET = 0;
    private static final int SLOT_LENGTH_OFFSET = 4;

    /**
     * Metode UTAMA untuk MEMBACA (digunakan oleh Orang 2 & 4).
     * Mengambil seluruh blok 4KB dan mengurai "Slotted Page"-nya 
     * untuk mengembalikan semua Row yang ada di dalamnya.
     */
    public List<Row> deserializeBlock(byte[] blockData) throws IOException {
        List<Row> rowsInBlock = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(blockData);

        // 1. Baca Header Blok
        int slotCount = buffer.getInt(HEADER_SLOT_COUNT_OFFSET);

        // 2. Iterasi melalui "Daftar Isi" (Slot Directory)
        for (int i = 0; i < slotCount; i++) {
            int slotOffset = BLOCK_HEADER_SIZE + (i * SLOT_SIZE);
            
            // 3. Baca info slot
            int dataOffset = buffer.getInt(slotOffset + SLOT_OFFSET_OFFSET);
            int dataLength = buffer.getInt(slotOffset + SLOT_LENGTH_OFFSET);

            // 4. Jika slot berisi data (bukan pointer 0)
            if (dataLength > 0 && dataOffset > 0) {
                byte[] rowBytes = new byte[dataLength];
                // Pergi ke lokasi data dan salin byte-nya
                System.arraycopy(blockData, dataOffset, rowBytes, 0, dataLength);
                
                // 5. Gunakan helper 'deserialize' untuk menerjemahkan byte[]
                rowsInBlock.add(deserialize(rowBytes));
            }
        }
        return rowsInBlock;
    }

    /**
     * Metode UTAMA untuk MENULIS (digunakan oleh Orang 2).
     * Mengambil blok 4KB yang ada, lalu "mengepak" Row baru ke dalamnya.
     * Mengembalikan blok 4KB yang sudah diperbarui.
     * * @param blockData Byte[] 4KB dari BlockManager
     * @param newRow Row baru yang ingin dimasukkan
     * @return Byte[] 4KB yang sudah diperbarui
     */
    public byte[] packRowToBlock(byte[] blockData, Row newRow) throws IOException {
        // 1. Ubah Row baru menjadi byte[]
        byte[] rowBytes = serialize(newRow);
        int rowLength = rowBytes.length;

        ByteBuffer buffer = ByteBuffer.wrap(blockData);

        // 2. Baca Header Blok
        int slotCount = buffer.getInt(HEADER_SLOT_COUNT_OFFSET);
        int freeSpaceOffset = buffer.getInt(HEADER_FREE_SPACE_OFFSET); // Pointer ke awal spasi kosong

        // 3. Cek apakah Row muat
        // Spasi yang dibutuhkan = Panjang data + panjang 1 slot baru
        int spaceNeeded = rowLength + SLOT_SIZE; 
        
        // Lokasi "Daftar Isi" (Slots) tumbuh ke bawah, Data tumbuh ke atas
        int nextSlotOffset = BLOCK_HEADER_SIZE + (slotCount * SLOT_SIZE);
        
        if (freeSpaceOffset - nextSlotOffset < spaceNeeded) {
            throw new IOException("Blok penuh, tidak cukup spasi untuk Row baru.");
        }

        // 4. Tulis data Row baru (dari belakang blok)
        int newDataOffset = freeSpaceOffset - rowLength;
        System.arraycopy(rowBytes, 0, blockData, newDataOffset, rowLength);

        // 5. Tulis "Slot" baru di "Daftar Isi"
        buffer.putInt(nextSlotOffset + SLOT_OFFSET_OFFSET, newDataOffset);
        buffer.putInt(nextSlotOffset + SLOT_LENGTH_OFFSET, rowLength);
        
        // 6. Perbarui Header Blok
        buffer.putInt(HEADER_SLOT_COUNT_OFFSET, slotCount + 1); // Tambah jumlah slot
        buffer.putInt(HEADER_FREE_SPACE_OFFSET, newDataOffset); // Geser pointer spasi kosong

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


    // ====================================================================
    // METODE ASLI (SEKARANG MENJADI HELPER)
    // ====================================================================

    /**
     * (HELPER) Serialize SATU Row object ke byte array.
     * TODO: Implementasikan logika konversi tipe data yang sebenarnya di sini.
     */
    public byte[] serialize(Row row) throws IOException {
        // TODO: Implementasi yang sebenarnya (misal: pakai DataOutputStream)
        // Ini hanya contoh dummy:
        String dummyData = row.data().toString();
        return dummyData.getBytes(); 
        
        // throw new UnsupportedOperationException("serialize not implemented yet");
    }
    
    /**
     * (HELPER) Deserialize byte array menjadi SATU Row object.
     * TODO: Implementasikan logika konversi tipe data yang sebenarnya di sini.
     */
    public Row deserialize(byte[] data) throws IOException {
        // TODO: Implementasi yang sebenarnya (misal: pakai DataInputStream)
        // Ini hanya contoh dummy:
        String dummyData = new String(data);
        // Ini parse dummy, Anda harus parse data asli jadi Map
        Map<String, Object> map = Map.of("data_dummy", dummyData); 
        return new Row(map);
        
        // throw new UnsupportedOperationException("deserialize not implemented yet");
    }
    
    /**
     * Calculate the estimated size of a serialized Row.
     * TODO: Implement size estimation logic
     */
    public int estimateSize(Row row) {
        // TODO: Implementasi ini sekarang sangat penting untuk 'packRowToBlock'
        
        // Contoh dummy:
        return row.data().toString().getBytes().length;
        
        // throw new UnsupportedOperationException("estimateSize not implemented yet");
    }
}