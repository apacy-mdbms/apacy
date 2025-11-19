package com.apacy.failurerecoverymanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogWriter {
    private final String logFilePath;
    private FileWriter writer;

    public LogWriter() {
        this("failure-recovery/log/mDBMS.log");
    }

    public LogWriter(String logFilePath) {
        this.logFilePath = logFilePath;

        try {
            initialize();
        } catch (IOException e) {
            throw new RuntimeException("Gagal inisialisasi LogWriter pada path " + logFilePath, e);
        }
    }

    private void initialize() throws IOException {
        File logFile = new File(logFilePath);
        File parentDir = logFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs(); 
        }
        writer = new FileWriter(logFile, true); 
    }

    /**
     * Menulis satu baris log transaksi.
     * Format: timestamp|transactionId|operation|tableName|data
     */
    public synchronized void writeLog(LogEntry entry) throws IOException {
        if (entry == null) return;

        writer.write(entry.toString());
        writer.write("\n");      
        writer.flush();         
    }

    public synchronized void writeLog(String transactionId, String operation, String tableName, Object data) throws IOException {
        LogEntry entry = new LogEntry(transactionId, operation, tableName, data);
        writeLog(entry);
    }

    /** Memaksa flush buffer ke file. */
    public void flush() throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }

    /** Menutup file writer dengan aman. */
    public void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

    /** Melakukan rotasi log dengan mengganti nama file log lama. */
    public void rotateLog() throws IOException {
        close();
        File oldFile = new File(logFilePath);
        File rotated = new File(logFilePath + "." + System.currentTimeMillis() + ".bak");
        if (oldFile.exists()) {
            oldFile.renameTo(rotated);
        }
        initialize(); 
    }
}
