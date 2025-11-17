package com.apacy.queryprocessor.cli;

import java.util.Scanner;
import java.util.List;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.common.dto.Row;
import com.apacy.queryprocessor.QueryProcessor;

/**
 * Interactive CLI for mDBMS-Apacy system.
 */
public class ApacyCLI {
    
    private final Scanner scanner;
    private final QueryProcessor queryProcessor;
    private boolean running;
    private String currentQuery;
    private final java.util.List<String> commandHistory;
    private int historyIndex;
    
    public ApacyCLI(QueryProcessor queryProcessor) {
        this.scanner = new Scanner(System.in);
        this.queryProcessor = queryProcessor;
        this.running = true;
        this.currentQuery = "";
        this.commandHistory = new java.util.ArrayList<>();
        this.historyIndex = -1;
    }
    
    public void start() {
        try {
            queryProcessor.initialize();
            showWelcomeMessage();
            System.out.println();
            
            while (running) {
                showPrompt();
                String input = readInputWithHistory();
                
                if (input == null || input.isEmpty()) {
                    continue;
                }
                
                processInput(input);
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }
    
    private String readInputWithHistory() {
        StringBuilder inputBuffer = new StringBuilder();
        int cursorPosition = 0; // Position within the input buffer
        
        try {
            // Enable raw mode for terminal to capture arrow keys properly
            ProcessBuilder pb = new ProcessBuilder("stty", "-echo", "raw");
            pb.inheritIO();
            Process sttyProcess = pb.start();
            sttyProcess.waitFor();
            
            try {
                while (true) {
                    int c = System.in.read();
                    
                    // Handle escape sequences (arrow keys)
                    if (c == 27) { // ESC character
                        byte[] escapeSeq = new byte[2];
                        int bytesRead = System.in.read(escapeSeq);
                        
                        if (bytesRead >= 2 && escapeSeq[0] == '[') {
                            if (escapeSeq[1] == 'A') { // Up arrow
                                String historyCommand = getHistoryUp();
                                if (historyCommand != null) {
                                    // Clear current input and display history command
                                    for (int i = 0; i < inputBuffer.length(); i++) {
                                        System.out.print("\b \b");
                                    }
                                    inputBuffer = new StringBuilder(historyCommand);
                                    cursorPosition = inputBuffer.length();
                                    System.out.print(historyCommand);
                                    System.out.flush();
                                }
                                continue;
                            } else if (escapeSeq[1] == 'B') { // Down arrow
                                String historyCommand = getHistoryDown();
                                if (historyCommand != null) {
                                    // Clear current input and display history command
                                    for (int i = 0; i < inputBuffer.length(); i++) {
                                        System.out.print("\b \b");
                                    }
                                    inputBuffer = new StringBuilder(historyCommand);
                                    cursorPosition = inputBuffer.length();
                                    System.out.print(historyCommand);
                                    System.out.flush();
                                } else if (historyCommand == null && historyIndex >= 0) {
                                    // Clear to empty when going below history
                                    for (int i = 0; i < inputBuffer.length(); i++) {
                                        System.out.print("\b \b");
                                    }
                                    inputBuffer = new StringBuilder();
                                    cursorPosition = 0;
                                    historyIndex = commandHistory.size();
                                    System.out.flush();
                                }
                                continue;
                            } else if (escapeSeq[1] == 'C') { // Right arrow
                                if (cursorPosition < inputBuffer.length()) {
                                    cursorPosition++;
                                    System.out.print("\033[C"); // Move cursor right
                                    System.out.flush();
                                }
                                continue;
                            } else if (escapeSeq[1] == 'D') { // Left arrow
                                if (cursorPosition > 0) {
                                    cursorPosition--;
                                    System.out.print("\033[D"); // Move cursor left
                                    System.out.flush();
                                }
                                continue;
                            }
                        }
                    }
                    
                    // Handle regular input
                    if (c == '\r') { // Carriage return
                        // Echo CR+LF to move to next line, column 1
                        System.out.print("\r\n");
                        System.out.flush();
                        String result = inputBuffer.toString();
                        if (!result.isEmpty()) {
                            commandHistory.add(result);
                            historyIndex = commandHistory.size();
                        }
                        return result;
                    } else if (c == '\n') { // Line feed only
                        // If we got LF without CR, treat it as enter too
                        System.out.print("\r\n");
                        System.out.flush();
                        String result = inputBuffer.toString();
                        if (!result.isEmpty()) {
                            commandHistory.add(result);
                            historyIndex = commandHistory.size();
                        }
                        return result;
                    } else if (c == 127 || c == '\b') { // Backspace
                        if (cursorPosition > 0) {
                            inputBuffer.deleteCharAt(cursorPosition - 1);
                            cursorPosition--;
                            // Move cursor left and erase the character
                            System.out.print("\b");
                            // Print the rest of the line and erase
                            String remainingText = inputBuffer.substring(cursorPosition);
                            System.out.print(remainingText + " ");
                            // Move cursor back to correct position
                            for (int i = 0; i < remainingText.length() + 1; i++) {
                                System.out.print("\b");
                            }
                            System.out.flush();
                        }
                    } else if (c == 3) { // Ctrl+C
                        System.out.print("\r\n");
                        System.out.flush();
                        running = false;
                        return "";
                    } else if (c >= 32 && c <= 126) { // Printable ASCII
                        inputBuffer.insert(cursorPosition, (char) c);
                        cursorPosition++;
                        // Print the character and everything after it
                        String remainingText = inputBuffer.substring(cursorPosition - 1);
                        System.out.print(remainingText);
                        // Move cursor back to the correct position
                        for (int i = 1; i < remainingText.length(); i++) {
                            System.out.print("\b");
                        }
                        System.out.flush();
                    }
                }
            } finally {
                // Restore terminal to cooked mode
                pb = new ProcessBuilder("stty", "echo", "-raw");
                pb.inheritIO();
                sttyProcess = pb.start();
                sttyProcess.waitFor();
            }
        } catch (Exception e) {
            System.err.println("Input error: " + e.getMessage());
            return scanner.nextLine().trim();
        }
    }
    
    private String getHistoryUp() {
        if (commandHistory.isEmpty()) {
            return null;
        }
        
        if (historyIndex > 0) {
            historyIndex--;
            return commandHistory.get(historyIndex);
        } else if (historyIndex == commandHistory.size()) {
            historyIndex = commandHistory.size() - 1;
            return commandHistory.get(historyIndex);
        }
        
        return null;
    }
    
    private String getHistoryDown() {
        if (commandHistory.isEmpty()) {
            return null;
        }
        
        if (historyIndex < commandHistory.size() - 1) {
            historyIndex++;
            return commandHistory.get(historyIndex);
        } else if (historyIndex == commandHistory.size() - 1) {
            historyIndex++;
            return null; // Move to empty state
        }
        
        return null;
    }
    
    private void showWelcomeMessage() {
        System.out.println("Welcome to mDBMS-Apacy interactive terminal!");
        System.out.println("Type 'help' for help, 'exit' or 'quit' to leave.");
        System.out.println();
    }
    
    private void showPrompt() {
        if (currentQuery.isEmpty()) {
            System.out.print("apacy=# ");
        } else {
            System.out.print("apacy-# ");
        }
    }
    
    private void processInput(String input) {
        // Handle special commands
        if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
            System.out.println("Goodbye!");
            running = false;
            return;
        }
        
        if (input.equalsIgnoreCase("help")) {
            showHelp();
            return;
        }
        
        if (input.equalsIgnoreCase("clear")) {
            clearScreen();
            return;
        }
        
        // Handle SQL queries
        currentQuery += input;
        
        // Check if query is complete (ends with semicolon)
        if (input.endsWith(";")) {
            executeQuery(currentQuery.substring(0, currentQuery.length() - 1).trim());
            currentQuery = "";
        } else {
            currentQuery += " ";
        }
    }
    
    private void executeQuery(String query) {
        if (query.trim().isEmpty()) {
            return;
        }
        
        System.out.println();
        
        try {
            long startTime = System.currentTimeMillis();
            ExecutionResult result = queryProcessor.executeQuery(query);
            long endTime = System.currentTimeMillis();
            
            displayResult(result);
            System.out.printf("Execution Time: %.3f ms%n", (endTime - startTime));
            
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }
        
        System.out.println();
    }
    
    private void displayResult(ExecutionResult result) {
        System.out.println("═══════════════════════════════════");
        System.out.println("Operation: " + result.operation());
        System.out.println("Success: " + (result.success() ? "True" : "False"));
        
        if (result.message() != null && !result.message().isEmpty()) {
            System.out.println("Message: " + result.message());
        }
        
        System.out.println("Transaction ID: " + result.transactionId());
        System.out.println("═══════════════════════════════════");
        
        if (result.rows() != null && !result.rows().isEmpty()) {
            displayTable(result.rows());
        }
        
        if (result.affectedRows() > 0) {
            System.out.println("\n " + result.affectedRows() + " row(s) affected");
        }
    }
    
    private void displayTable(List<Row> rows) {
        if (rows.isEmpty()) {
            System.out.println("(0 rows)");
            return;
        }
        
        System.out.println();
        
        // Get column names from first row
        java.util.List<String> columnNames = new java.util.ArrayList<>(rows.get(0).data().keySet());
        
        // Calculate column widths
        java.util.Map<String, Integer> columnWidths = new java.util.LinkedHashMap<>();
        for (String col : columnNames) {
            int width = col.length();
            for (Row row : rows) {
                Object value = row.get(col);
                int valueLength = value != null ? value.toString().length() : 4;
                width = Math.max(width, valueLength);
            }
            columnWidths.put(col, Math.max(width, 4));
        }
        
        // Print header
        printTableHeader(columnNames, columnWidths);
        
        // Print separator
        printTableSeparator(columnNames, columnWidths);
        
        // Print rows
        for (Row row : rows) {
            printTableRow(row, columnNames, columnWidths);
        }
        
        System.out.println("(" + rows.size() + " row" + (rows.size() == 1 ? "" : "s") + ")");
    }
    
    private void printTableHeader(java.util.List<String> columnNames, java.util.Map<String, Integer> columnWidths) {
        StringBuilder header = new StringBuilder();
        boolean first = true;
        for (String col : columnNames) {
            if (!first) {
                header.append(" | ");
            }
            header.append(String.format("%-" + columnWidths.get(col) + "s", col));
            first = false;
        }
        System.out.println(header.toString());
    }
    
    private void printTableSeparator(java.util.List<String> columnNames, java.util.Map<String, Integer> columnWidths) {
        StringBuilder separator = new StringBuilder();
        boolean first = true;
        for (String col : columnNames) {
            if (!first) {
                separator.append("─+─");
            }
            separator.append("─".repeat(columnWidths.get(col)));
            first = false;
        }
        System.out.println(separator.toString());
    }
    
    private void printTableRow(Row row, java.util.List<String> columnNames, java.util.Map<String, Integer> columnWidths) {
        StringBuilder rowStr = new StringBuilder();
        boolean first = true;
        for (String col : columnNames) {
            if (!first) {
                rowStr.append(" | ");
            }
            Object value = row.get(col);
            String valueStr = value != null ? value.toString() : "(null)";
            rowStr.append(String.format("%-" + columnWidths.get(col) + "s", valueStr));
            first = false;
        }
        System.out.println(rowStr.toString());
    }
    
    private void showHelp() {
        System.out.println("\n" + "═".repeat(50));
        System.out.println("           mDBMS-Apacy Help");
        System.out.println("═".repeat(50));
        System.out.println();
        System.out.println("SQL Commands (end with ;):");
        System.out.println("  SELECT * FROM table_name;");
        System.out.println("  INSERT INTO table VALUES (...);");
        System.out.println("  UPDATE table SET column=value WHERE ...;");
        System.out.println("  DELETE FROM table WHERE ...;");
        System.out.println();
        System.out.println("CLI Commands:");
        System.out.println("  help      - Show this help");
        System.out.println("  clear     - Clear screen");
        System.out.println("  exit      - Exit program");
        System.out.println("  quit      - Exit program");
        System.out.println();
        System.out.println("Note: Using real Storage Manager, Query Optimizer,");
        System.out.println("      Concurrency Control, and Failure Recovery!");
        System.out.println("═".repeat(50));
        System.out.println();
    }
    
    private void clearScreen() {
        // ANSI escape codes to clear screen
        System.out.print("\033[2J\033[H");
        System.out.flush();
        System.out.println("=================================");
        System.out.println("  mDBMS-Apacy - Screen Cleared");
        System.out.println("=================================");
        System.out.println();
    }
    
    private void cleanup() {
        try {
            System.out.println("\nShutting down mDBMS-Apacy...");
            queryProcessor.shutdown();
            scanner.close();
            System.out.println("Shutdown complete!");
        } catch (Exception e) {
            System.err.println("Cleanup error: " + e.getMessage());
        }
    }
}