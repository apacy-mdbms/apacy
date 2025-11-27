package com.apacy.queryprocessor;

import com.apacy.queryprocessor.client.ApacyCLI;
import com.apacy.queryprocessor.server.ApacyServer;

/**
 * Main entry point for the mDBMS-Apacy system.
 * Supports both server and client mode based on command line arguments.
 */
public class Main {
    
    public static void main(String[] args) {
        System.out.println("=================================");
        System.out.println("  mDBMS-Apacy - Super Group Apacy");
        System.out.println("  Modular Database Management System");
        System.out.println("=================================");
        System.out.println();
        
        // Parse command line arguments
        if (args.length > 0 && "server".equalsIgnoreCase(args[0])) {
            // Start server mode
            runServer();
        } else {
            // Start client mode (default)
            runClient();
        }
    }
    
    /**
     * Run in server mode
     */
    private static void runServer() {
        System.out.println("Starting in SERVER mode...");
        System.out.println();
        
        ApacyServer server = new ApacyServer();
        
        // Setup shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            server.stop();
        }));
        
        try {
            server.start();
        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Run in client mode
     */
    private static void runClient() {
        System.out.println("Starting in CLIENT mode...");
        System.out.println();
        
        ApacyCLI client = new ApacyCLI();
        client.start();
    }
}