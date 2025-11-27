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
        writer.write(entry.toString());
        writer.write("\n");
        writer.flush();
    }

    /**
     * Backwards compatible helper (old-style)
     */
    public synchronized void writeLog(String transactionId, String operation, String tableName, Object dataAfter) throws IOException {
        LogEntry entry = new LogEntry(transactionId, operation, tableName, null, dataAfter);
        writeLog(entry);
    }

    /** Memaksa flush buffer ke file. */
    public synchronized void flush() throws IOException {
        if (writer != null) writer.flush();
    }

    /** Menutup file writer dengan aman. */
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
            writer = null;
        }
    }

    /** Mengembalikan lokasi file WAL yang sedang digunakan. */
    public String getLogFilePath() {
        return logFilePath;
    }

    /** Melakukan rotasi / archive log: rename current log and initialize a new one. */
    public synchronized void rotateLog() throws IOException {
        close();
        File oldFile = new File(logFilePath);
        if (oldFile.exists()) {
            File rotated = new File(logFilePath + "." + System.currentTimeMillis() + ".bak");
            if (!oldFile.renameTo(rotated)) {
                throw new IOException("Gagal merename log file untuk rotation");
            }
        }
        initialize();
    }
}
