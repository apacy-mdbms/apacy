package com.apacy.failurerecoverymanager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogWriter {
    private final String logFilePath;
    private FileWriter writer;

    public LogWriter() throws IOException {
        this("failure-recovery/log/mDBMS.log");
    }

    public LogWriter(String logFilePath) throws IOException {
        this.logFilePath = logFilePath;
        initialize();
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
    public synchronized void writeLog(String transactionId, String operation, String tableName, Object data) throws IOException {
        long timestamp = System.currentTimeMillis();
        String safeData = (data != null ? data.toString().replace("\n", " ").replace("|", " ") : "-");
        String logLine = timestamp + "|" +
                (transactionId != null ? transactionId : "-") + "|" +
                (operation != null ? operation : "-") + "|" +
                (tableName != null ? tableName : "-") + "|" +
                safeData + "\n";

        writer.write(logLine);
        writer.flush(); 
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
