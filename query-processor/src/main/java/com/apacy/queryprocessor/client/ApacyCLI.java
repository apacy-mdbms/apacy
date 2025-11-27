package com.apacy.queryprocessor.client;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.Row;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;

/**
 * mDBMS Apacy Client CLI - Interface untuk berinteraksi dengan server database
 */
public class ApacyCLI {
    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 8888;
    
    private Socket socket;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;
    private Scanner scanner;
    private boolean connected = false;
    
    public ApacyCLI() {
        this.scanner = new Scanner(System.in);
    }
    
    /**
     * Start CLI dan connect ke server
     */
    public void start() {
        System.out.println("=================================");
        System.out.println("  mDBMS-Apacy Client Terminal");
        System.out.println("=================================");
        System.out.println();
        
        if (!connectToServer()) {
            System.err.println("Failed to connect to server. Exiting...");
            return;
        }
        
        System.out.println("Welcome to mDBMS-Apacy interactive terminal!");
        System.out.println("Type 'help' for help, 'exit' or 'quit' to leave.");
        System.out.println();
        
        // Main CLI loop
        runInteractiveLoop();
        
        disconnect();
    }
    
    /**
     * Connect ke server database
     */
    private boolean connectToServer() {
        try {
            System.out.println("Connecting to server at " + SERVER_HOST + ":" + SERVER_PORT + "...");
            
            socket = new Socket(SERVER_HOST, SERVER_PORT);
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            inputStream = new ObjectInputStream(socket.getInputStream());
            
            connected = true;
            System.out.println("Connected to server successfully!");
            System.out.println();
            
            return true;
            
        } catch (ConnectException e) {
            System.err.println("Connection refused. Please make sure the server is running on port " + SERVER_PORT);
            return false;
        } catch (IOException e) {
            System.err.println("Failed to connect to server: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Main interactive loop untuk input user
     */
    private void runInteractiveLoop() {
        while (connected) {
            System.out.print("apacy=# ");
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) {
                continue;
            }
            
            // Handle special commands
            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                break;
            } else if (input.equalsIgnoreCase("help")) {
                showHelp();
                continue;
            } else if (input.equalsIgnoreCase("\\q")) {
                break;
            }
            
            // Execute SQL query
            executeQuery(input);
        }
    }
    
    /**
     * Execute query dengan mengirim ke server
     */
    private void executeQuery(String sqlQuery) {
        try {
            long startTime = System.nanoTime();
            
            // Kirim query ke server
            outputStream.writeObject(sqlQuery);
            outputStream.flush();
            
            // Terima hasil dari server
            ExecutionResult result = (ExecutionResult) inputStream.readObject();
            
            long endTime = System.nanoTime();
            double executionTime = (endTime - startTime) / 1_000_000.0; // Convert to milliseconds
            
            // Display hasil
            displayResult(result, executionTime);
            
        } catch (IOException e) {
            System.err.println("Communication error with server: " + e.getMessage());
            connected = false;
        } catch (ClassNotFoundException e) {
            System.err.println("Invalid response from server: " + e.getMessage());
        }
    }
    
    /**
     * Display hasil query ke user
     */
    private void displayResult(ExecutionResult result, double executionTime) {
        System.out.println();
        
        if (result.success()) {
            // Display hasil query berhasil
            if ("SELECT".equalsIgnoreCase(result.operation()) && result.rows() != null && !result.rows().isEmpty()) {
                displaySelectResult(result.rows());
            } else {
                System.out.println("═══════════════════════════════════");
                System.out.println("Operation: " + result.operation());
                System.out.println("Success: " + result.success());
                System.out.println("Message: " + result.message());
                if (result.transactionId() > 0) {
                    System.out.println("Transaction ID: " + result.transactionId());
                }
                System.out.println("═══════════════════════════════════");
                
                if (result.affectedRows() > 0) {
                    System.out.println(result.affectedRows() + " row(s) affected");
                }
            }
        } else {
            // Display error
            System.err.println("ERROR: " + result.message());
        }
        
        System.out.println("Execution Time: " + String.format("%.3f", executionTime) + " ms");
        System.out.println();
    }
    
    /**
     * Display hasil SELECT dalam format tabel
     */
    private void displaySelectResult(List<Row> rows) {
        if (rows.isEmpty()) {
            System.out.println("(0 rows)");
            return;
        }
        
        // Get column names from first row
        Row firstRow = rows.get(0);
        List<String> columnNames = firstRow.data().keySet().stream().sorted().toList();
        
        // Calculate column widths
        int[] columnWidths = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            columnWidths[i] = columnName.length();
            
            // Check max width needed for this column
            for (Row row : rows) {
                Object value = row.get(columnName);
                String valueStr = (value != null) ? value.toString() : "";
                columnWidths[i] = Math.max(columnWidths[i], valueStr.length());
            }
            
            // Minimum width of 8
            columnWidths[i] = Math.max(columnWidths[i], 8);
        }
        
        // Print header
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) System.out.print(" | ");
            System.out.printf("%-" + columnWidths[i] + "s", columnNames.get(i));
        }
        System.out.println();
        
        // Print separator
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) System.out.print("─+─");
            System.out.print("─".repeat(columnWidths[i]));
        }
        System.out.println();
        
        // Print data rows
        for (Row row : rows) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (i > 0) System.out.print(" | ");
                Object value = row.get(columnNames.get(i));
                String valueStr = (value != null) ? value.toString() : "";
                System.out.printf("%-" + columnWidths[i] + "s", valueStr);
            }
            System.out.println();
        }
        
        System.out.println("(" + rows.size() + " rows)");
        System.out.println();
        System.out.println(rows.size() + " row(s) affected");
    }
    
    /**
     * Show help message
     */
    private void showHelp() {
        System.out.println();
        System.out.println("Available commands:");
        System.out.println("  SELECT * FROM table_name;     - Query data");
        System.out.println("  INSERT INTO table ...;        - Insert data");
        System.out.println("  UPDATE table SET ...;         - Update data");
        System.out.println("  DELETE FROM table ...;        - Delete data");
        System.out.println("  CREATE TABLE ...;             - Create table");
        System.out.println("  DROP TABLE table_name;        - Drop table");
        System.out.println("  help                          - Show this help");
        System.out.println("  exit, quit, \\q                - Exit client");
        System.out.println();
    }
    
    /**
     * Disconnect dari server
     */
    private void disconnect() {
        if (connected) {
            try {
                // Send exit command to server
                outputStream.writeObject("exit");
                outputStream.flush();
                
                // Read final response
                try {
                    ExecutionResult result = (ExecutionResult) inputStream.readObject();
                    if (result.message() != null) {
                        System.out.println(result.message());
                    }
                } catch (Exception e) {
                    // Ignore errors during shutdown
                }
                
            } catch (IOException e) {
                // Ignore errors during shutdown
            }
        }
        
        // Close resources
        try {
            if (inputStream != null) inputStream.close();
            if (outputStream != null) outputStream.close();
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException e) {
            // Ignore errors during cleanup
        }
        
        connected = false;
        System.out.println("Disconnected from server.");
    }
    
    /**
     * Main method untuk menjalankan client
     */
    public static void main(String[] args) {
        ApacyCLI client = new ApacyCLI();
        client.start();
    }
}