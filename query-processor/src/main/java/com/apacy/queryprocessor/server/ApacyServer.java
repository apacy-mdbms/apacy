package com.apacy.queryprocessor.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.apacy.concurrencycontrolmanager.ConcurrencyControlManager;
import com.apacy.failurerecoverymanager.FailureRecoveryManager;
import com.apacy.queryoptimizer.QueryOptimizer;
import com.apacy.queryprocessor.QueryProcessor;
import com.apacy.storagemanager.StorageManager;
import com.apacy.queryprocessor.ServerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * mDBMS Apacy Server - Mengelola koneksi client dan menjalankan komponen database utama
 */
public class ApacyServer {
    private static final int PORT = 8888;
    private static final int THREAD_POOL_SIZE = 10;
    
    private ServerSocket serverSocket;
    private ExecutorService threadPool;
    private QueryProcessor queryProcessor;
    private boolean isRunning = false;
    
    public ApacyServer() {
        this.threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    }
    
    /**
     * Inisialisasi semua komponen database dan start server
     */
    public void start() throws Exception {
        System.out.println("=================================");
        System.out.println("  mDBMS-Apacy Server Starting...");
        System.out.println("=================================");
        
        // Inisialisasi komponen database
        initializeComponents();
        
        // Start server socket
        serverSocket = new ServerSocket(PORT);
        isRunning = true;
        
        System.out.println("Server started on port " + PORT);
        System.out.println("Waiting for client connections...");
        
        // Loop untuk menerima koneksi client
        while (isRunning) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());
                
                // Submit client handling ke thread pool
                threadPool.submit(new ClientWorker(clientSocket, queryProcessor));
                
            } catch (IOException e) {
                if (isRunning) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Inisialisasi semua komponen database
     */
    private void initializeComponents() throws Exception {
        System.out.println("Initializing database components...");

        ServerConfig config = new ServerConfig(); // Default "lock"
        try {
            File configFile = new File("config.json");
            if (configFile.exists()) {
                ObjectMapper mapper = new ObjectMapper();
                config = mapper.readValue(configFile, ServerConfig.class);
                System.out.println("[Config] Loaded Algorithm: " + config.getAlgorithm());
            } else {
                System.out.println("[Config] config.json not found. Using default: " + config.getAlgorithm());
            }
        } catch (Exception e) {
            System.err.println("[Config] Failed to read config: " + e.getMessage());
        }
        
        // Inisialisasi storage manager
        StorageManager storageManager = new StorageManager("../data");
        storageManager.initialize();
        
        // Inisialisasi komponen lainnya
        QueryOptimizer queryOptimizer = new QueryOptimizer();
        FailureRecoveryManager recoveryManager = new FailureRecoveryManager(storageManager);
        ConcurrencyControlManager concurrencyManager = new ConcurrencyControlManager(
            config.getAlgorithm(), 
            recoveryManager
        );
        
        // Inisialisasi query processor dengan semua komponen
        queryProcessor = new QueryProcessor(
            queryOptimizer,
            storageManager,
            concurrencyManager,
            recoveryManager
        );
        
        queryProcessor.initialize();
        
        System.out.println("All database components initialized successfully!");
    }
    
    /**
     * Stop server dan cleanup resources
     */
    public void stop() {
        System.out.println("Stopping mDBMS-Apacy Server...");
        isRunning = false;
        
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }
        
        if (threadPool != null && !threadPool.isShutdown()) {
            threadPool.shutdown();
        }
        
        if (queryProcessor != null) {
            queryProcessor.shutdown();
        }
        
        System.out.println("Server stopped successfully.");
    }
    
    /**
     * Main method untuk menjalankan server
     */
    public static void main(String[] args) {
        ApacyServer server = new ApacyServer();
        
        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        try {
            server.start();
        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}