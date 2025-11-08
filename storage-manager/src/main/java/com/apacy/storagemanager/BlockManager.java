package com.apacy.storagemanager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Boilerplate Block Manager for low-level block operations.
 * TODO: Implement reading and writing of fixed-size data blocks to disk.
 */
public class BlockManager {
    
    private static final int DEFAULT_BLOCK_SIZE = 4096; // 4KB blocks
    
    private final String dataDirectory;
    private final int blockSize;
    
    public BlockManager(String dataDirectory) {
        this(dataDirectory, DEFAULT_BLOCK_SIZE);
    }
    
    public BlockManager(String dataDirectory, int blockSize) {
        this.dataDirectory = dataDirectory;
        this.blockSize = blockSize;
        // TODO: Initialize file management structures and create directory if needed

        try{
            Files.createDirectories(Paths.get(this.dataDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Gagal membuat direktori data : " + dataDirectory, e);
        }
    }

    // Helper //
    private Path getFilePath(String fileName) {
        return Paths.get(dataDirectory, fileName);
    }
    
    /**
     * Read a block from the specified file at the given block number.
     * TODO: Implement block reading with proper error handling and buffering
     */
    public byte[] readBlock(String fileName, long blockNumber) throws IOException {
        // TODO: Implement block reading logic
        Path filePath = getFilePath(fileName);
        if(!Files.exists(filePath)) {
            throw new FileNotFoundException("File tidak ditemukan : " + fileName);
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            long fileLength = raf.length();
            long position = blockNumber * blockSize;

            if (position >= fileLength) {
                throw new IOException("Nomor blok " + blockNumber + " di luar batas file " + fileName + " (panjang: " + fileLength + " bytes)");
            }

            raf.seek(position);

            byte[] blockData = new byte[blockSize];
            int bytesRead = raf.read(blockData);

            if (bytesRead < blockSize && bytesRead != -1) {
                // Ini bisa terjadi jika blok terakhir tidak penuh (corrupt/partial write)
                // Kita kembalikan data yang terbaca, sisa array akan 0 (default)
                System.err.println("Peringatan: readBlock membaca kurang dari ukuran blok penuh di " + fileName + ", blok " + blockNumber);
            }

            return blockData;
        }
        // throw new UnsupportedOperationException("readBlock not implemented yet");
    }
    
    /**
     * Write a block to the specified file at the given block number.
     * TODO: Implement block writing with proper padding and synchronization
     */
    public void writeBlock(String fileName, long blockNumber, byte[] data) throws IOException {
        // TODO: Implement block writing logic
        if (data.length > blockSize) {
            throw new IOException("Data ( " + data.length + " bytes) lebih besar dari blockSize (" + blockSize + " bytes)");
        }

        Path filePath = getFilePath(fileName);
        
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")) {
            long position = blockNumber * blockSize;
            
            // Pindah ke posisi blok
            raf.seek(position);
            
            // Tulis data
            raf.write(data);
            
            // (PENTING) Jika data lebih kecil dari ukuran blok,
            // kita harus menulis padding agar file tetap sinkron.
            if (data.length < blockSize) {
                byte[] padding = new byte[blockSize - data.length];
                // Arrays.fill(padding, (byte) 0); // (default-nya sudah 0)
                raf.write(padding);
            }
        }
        // throw new UnsupportedOperationException("writeBlock not implemented yet");
    }
    
    /**
     * Append a new block to the end of the specified file.
     * TODO: Implement block appending and return new block number
     */
    public long appendBlock(String fileName, byte[] data) throws IOException {
        // TODO: Implement block appending logic
        long newBlockNumber = getBlockCount(fileName);
        writeBlock(fileName, newBlockNumber, data);
        return newBlockNumber;
        // throw new UnsupportedOperationException("appendBlock not implemented yet");
    }
    
    /**
     * Get the number of blocks in the specified file.
     * TODO: Implement block count calculation
     */
    public long getBlockCount(String fileName) throws IOException {
        // TODO: Implement block count logic
        Path filePath = getFilePath(fileName);
        if (!Files.exists(filePath)) {
            return 0;
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")){
            long fileLength = raf.length();
            if(fileLength == 0 ){
                return 0;
            }
            return (fileLength + blockSize - 1) / blockSize;
        }
    }
    
    /**
     * Flush all pending writes to disk.
     * TODO: Implement flush operation for durability
     */
    public void flush() throws IOException {
        // TODO: Implement flush logic
        throw new UnsupportedOperationException("flush not implemented yet");
    }
    
    /**
     * Close all open files and release resources.
     * TODO: Implement proper resource cleanup
     */
    public void close() {
        // TODO: Implement resource cleanup
        throw new UnsupportedOperationException("close not implemented yet");
    }
    
    /**
     * Get the block size used by this BlockManager.
     */
    public int getBlockSize() {
        return blockSize;
    }
    
    /**
     * Get the data directory path.
     */
    public String getDataDirectory() {
        return dataDirectory;
    }
}