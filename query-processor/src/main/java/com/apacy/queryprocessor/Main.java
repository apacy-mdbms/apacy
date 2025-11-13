package com.apacy.queryprocessor;

// Import real components from each module
import com.apacy.storagemanager.StorageManager;
import com.apacy.queryoptimizer.QueryOptimizer;
import com.apacy.concurrencycontrolmanager.ConcurrencyControlManager;
import com.apacy.failurerecoverymanager.FailureRecoveryManager;
import com.apacy.queryprocessor.cli.ApacyCLI;

/**
 * Main entry point for the mDBMS-Apacy system.
 * Handles component initialization and launches the CLI interface.
 */
public class Main {
    
    public static void main(String[] args) {
        System.out.println("=================================");
        System.out.println("  mDBMS-Apacy - Super Group Apacy");
        System.out.println("  Modular Database Management System");
        System.out.println("=================================");
        System.out.println();
        
        try {
            System.out.println("Initializing components...");
            
            StorageManager storageManager = new StorageManager("./storage-manager/data");
            QueryOptimizer queryOptimizer = new QueryOptimizer();
            ConcurrencyControlManager concurrencyManager = new ConcurrencyControlManager();
            FailureRecoveryManager recoveryManager = new FailureRecoveryManager();
            
            // Initialize QueryProcessor with real components
            QueryProcessor queryProcessor = new QueryProcessor(
                queryOptimizer,
                storageManager,
                concurrencyManager,
                recoveryManager
            );
            
            System.out.println("All components initialized successfully!");
            System.out.println();
            
            ApacyCLI cli = new ApacyCLI(queryProcessor);
            cli.start();
            
        } catch (Exception e) {
            System.err.println("Failed to initialize components: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}