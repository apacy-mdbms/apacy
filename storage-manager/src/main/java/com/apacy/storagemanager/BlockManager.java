package com.apacy.storagemanager;

import java.io.*;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Boilerplate Block Manager for low-level block operations.
 * TODO: Implement reading and writing of fixed-size data blocks to disk.
 */
public class BlockManager {
    
    public static final int DEFAULT_BLOCK_SIZE = 4096; // 4KB blocks
    
    private final String dataDirectory;
    private final int blockSize;

    private final Map<String, RandomAccessFile> openFiles;
    
    public BlockManager(String dataDirectory) {
        this(dataDirectory, DEFAULT_BLOCK_SIZE);
    }
    
    public BlockManager(String dataDirectory, int blockSize) {
        this.dataDirectory = dataDirectory;
        this.blockSize = blockSize;
        this.openFiles = new ConcurrentHashMap<>();
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
     * Helper: Dapatkan file handle yang sudah terbuka, atau buka baru jika belum ada.
     */
    private synchronized RandomAccessFile getOpenFile(String fileName) throws IOException {
        RandomAccessFile raf = openFiles.get(fileName);

        if (raf == null) {
            Path filePath = getFilePath(fileName);
            // Mode "rw" = Read & Write
            raf = new RandomAccessFile(filePath.toFile(), "rw");
            openFiles.put(fileName, raf);
        }
        return raf;
    }

    /**
     * Read a block from the specified file at the given block number.
     * TODO: Implement block reading with proper error handling and buffering
     */
    public byte[] readBlock(String fileName, long blockNumber) throws IOException {
        // TODO: Implement block reading logic
        RandomAccessFile raf = getOpenFile(fileName);
        long position = blockNumber * blockSize;
        byte[] blockData = new byte[blockSize];

        synchronized (raf) {
            long fileLength = raf.length();
            if (position >= fileLength) {
                throw new IOException("Nomor blok " + blockNumber + " di luar batas file " + fileName);
            }

            raf.seek(position);

            try {
                raf.readFully(blockData);
            } catch (EOFException e) {
                // Blok tidak penuh/korup, lempar error atau biarkan sisa 0
                throw new IOException("Blok " + blockNumber + " korup/tidak lengkap di " + fileName, e);
            }
        }
        return blockData;
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

        RandomAccessFile raf = getOpenFile(fileName);
        long position = blockNumber * blockSize;
        
       synchronized (raf) {
            raf.seek(position);
            raf.write(data);

            if (data.length < blockSize) {
                byte[] padding = new byte[blockSize - data.length];
                raf.write(padding);
            }
        }
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

        // Gunakan file yang terbuka untuk cek panjang (lebih cepat)
        RandomAccessFile raf = getOpenFile(fileName);
        long fileLength;

        synchronized (raf) {
            fileLength = raf.length();
        }

        if (fileLength == 0) {
            return 0;
        }
        return (fileLength + blockSize - 1) / blockSize;
    }
    
    /**
     * Flush all pending writes to disk.
     * TODO: Implement flush operation for durability
     */
    public void flush() throws IOException {
        // TODO: Implement flush logic
        System.out.println("BlockManager (Stateful): Flushing " + openFiles.size() + " files...");
        for (RandomAccessFile raf : openFiles.values()) {
            synchronized (raf) {
                raf.getChannel().force(true); // Force write to disk
            }
        }
    }
    
    /**
     * Close all open files and release resources.
     * TODO: Implement proper resource cleanup
     */
    public void close() throws IOException{
        // TODO: Implement resource cleanup
        System.out.println("BlockManager (Stateful): Closing " + openFiles.size() + " files...");
        
        // Flush dulu untuk keamanan data
        flush();

        for (Map.Entry<String, RandomAccessFile> entry : openFiles.entrySet()) {
            try {
                synchronized (entry.getValue()) {
                    entry.getValue().close();
                }
            } catch (IOException e) {
                System.err.println("Gagal menutup file: " + entry.getKey());
            }
        }
        openFiles.clear();
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