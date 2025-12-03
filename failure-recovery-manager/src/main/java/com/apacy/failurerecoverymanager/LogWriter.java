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

    public synchronized void writeLog(LogEntry entry) throws IOException {
        if (entry == null) return;
        writer.write(entry.toString()); // JSON format
        writer.write("\n");
        writer.flush();
    }


     // Helper buat log kebalik
    public synchronized void writeLog(String transactionId, String operation, String tableName, Object dataAfter) throws IOException {
        LogEntry entry = new LogEntry(transactionId, operation, tableName, null, dataAfter);
        writeLog(entry);
    }

    // Memaksa flush buffer ke file.
    public synchronized void flush() throws IOException {
        if (writer != null) writer.flush();
    }

    // Menutup file writer dengan aman.
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
            writer = null;
        }
    }

    // Mengembalikan lokasi file WAL yang sedang digunakan.
    public String getLogFilePath() {
        return logFilePath;
    }

    // Melakukan rotasi / archive log: rename current log and initialize a new one.
    public synchronized void rotateLog() throws IOException {
        close();
        File oldFile = new File(logFilePath);
        if (oldFile.exists()) {
            File rotated = new File(logFilePath + "." + System.currentTimeMillis() + ".bak");
            // Try to rename, but if it fails, just delete the old file
            if (!oldFile.renameTo(rotated)) {
                // If rename fails, try to delete the file
                if (!oldFile.delete()) {
                    System.err.println("Gagal merename atau menghapus log file untuk rotation");
                    // Continue anyway - don't throw exception
                }
            }
        }
        initialize();
    }

    // Check if log file exists and contains data
    public boolean hasLogs() {
        File logFile = new File(logFilePath);
        return logFile.exists() && logFile.length() > 0;
    }
}
