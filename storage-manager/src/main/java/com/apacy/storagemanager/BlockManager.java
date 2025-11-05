package com.apacy.storagemanager;

import java.io.*;

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
    }
    
    /**
     * Read a block from the specified file at the given block number.
     * TODO: Implement block reading with proper error handling and buffering
     */
    public byte[] readBlock(String fileName, long blockNumber) throws IOException {
        // TODO: Implement block reading logic
        throw new UnsupportedOperationException("readBlock not implemented yet");
    }
    
    /**
     * Write a block to the specified file at the given block number.
     * TODO: Implement block writing with proper padding and synchronization
     */
    public void writeBlock(String fileName, long blockNumber, byte[] data) throws IOException {
        // TODO: Implement block writing logic
        throw new UnsupportedOperationException("writeBlock not implemented yet");
    }
    
    /**
     * Append a new block to the end of the specified file.
     * TODO: Implement block appending and return new block number
     */
    public long appendBlock(String fileName, byte[] data) throws IOException {
        // TODO: Implement block appending logic
        throw new UnsupportedOperationException("appendBlock not implemented yet");
    }
    
    /**
     * Get the number of blocks in the specified file.
     * TODO: Implement block count calculation
     */
    public long getBlockCount(String fileName) throws IOException {
        // TODO: Implement block count logic
        throw new UnsupportedOperationException("getBlockCount not implemented yet");
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