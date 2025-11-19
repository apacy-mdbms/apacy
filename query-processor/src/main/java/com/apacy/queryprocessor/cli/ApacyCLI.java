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
    
    public ApacyCLI(QueryProcessor queryProcessor) {
        this.scanner = new Scanner(System.in);
        this.queryProcessor = queryProcessor;
        this.running = true;
        this.currentQuery = "";
    }
    
    public void start() {
        try {
            queryProcessor.initialize();
            showWelcomeMessage();
            System.out.println();
            
            while (running) {
                showPrompt();
                String input = scanner.nextLine().trim();
                
                if (input.isEmpty()) {
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
        
        System.out.println("\nQuery Results:");
        System.out.println("┌" + "─".repeat(60) + "┐");
        
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            System.out.println("│ Row " + (i + 1) + ": " + formatRowCompact(row));
        }
        
        System.out.println("└" + "─".repeat(60) + "┘");
        System.out.println("(" + rows.size() + " row(s) returned)");
    }
    
    private String formatRowCompact(Row row) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        
        for (String key : row.data().keySet()) {
            if (!first) {
                sb.append(", ");
            }
            Object value = row.get(key);
            sb.append(key).append("=").append(value);
            first = false;
        }
        
        // Ensure the string fits in the table width
        String result = sb.toString();
        if (result.length() > 50) {
            result = result.substring(0, 47) + "...";
        }
        
        return String.format("%-50s", result) + " │";
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