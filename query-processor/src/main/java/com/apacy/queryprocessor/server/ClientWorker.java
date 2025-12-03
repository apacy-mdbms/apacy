package com.apacy.queryprocessor.server;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;

import com.apacy.common.dto.ExecutionResult;
import com.apacy.queryprocessor.QueryProcessor;

/**
 * Worker thread yang menangani satu koneksi client
 */
public class ClientWorker implements Runnable {
    private final Socket clientSocket;
    private final QueryProcessor queryProcessor;
    private ObjectInputStream inputStream;
    private ObjectOutputStream outputStream;
    private int currentTxId = -1;
    
    public ClientWorker(Socket clientSocket, QueryProcessor queryProcessor) {
        this.clientSocket = clientSocket;
        this.queryProcessor = queryProcessor;
    }
    
    @Override
    public void run() {
        try {
            // Setup input/output streams
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            
            String clientAddress = clientSocket.getInetAddress().toString();
            System.out.println("ClientWorker started for: " + clientAddress);
            
            // Loop untuk menangani request dari client
            while (!clientSocket.isClosed()) {
                try {
                    // Baca SQL query dari client
                    String sqlQuery = (String) inputStream.readObject();
                    
                    if (sqlQuery == null) {
                        break; // Client disconnected
                    }
                    
                    System.out.println("Received query from " + clientAddress + ": " + sqlQuery);
                    
                    // Handle special commands
                    if ("exit".equalsIgnoreCase(sqlQuery.trim()) || "quit".equalsIgnoreCase(sqlQuery.trim())) {
                        // Client wants to disconnect
                        ExecutionResult result = new ExecutionResult(
                            true, "Goodbye!", 0, "EXIT", 0, null
                        );
                        outputStream.writeObject(result);
                        outputStream.flush();
                        break;
                    }
                    
                    // Execute query menggunakan QueryProcessor
                    ExecutionResult result;
                    try {
                        result = queryProcessor.executeQuery(sqlQuery, currentTxId);
                        if (result.success()) {
                            String op = result.operation().toUpperCase();
                            
                            if (op.equals("BEGIN") || op.equals("BEGIN TRANSACTION")) {
                                this.currentTxId = result.transactionId();
                                System.out.println("Session " + clientSocket.getInetAddress() + " started Tx: " + currentTxId);
                            } 
                            else if (op.equals("COMMIT") || op.equals("ROLLBACK") || op.equals("ABORT")) {
                                System.out.println("Session " + clientSocket.getInetAddress() + " finished Tx: " + currentTxId);
                                this.currentTxId = -1;
                            }
                        } else {
                            if (this.currentTxId != -1) {
                                System.out.println("Session " + clientSocket.getInetAddress() + " Tx " + currentTxId + " aborted by server due to error.");
                                this.currentTxId = -1;
                            }
                        }
                    } catch (Exception e) {
                        // Handle execution error
                        result = new ExecutionResult(
                            false, "Query execution error: " + e.getMessage(), 
                            0, "ERROR", 0, null
                        );
                        e.printStackTrace();

                        if (this.currentTxId != -1) {
                            this.currentTxId = -1;
                        }
                    }
                    
                    // Kirim hasil kembali ke client
                    outputStream.writeObject(result);
                    outputStream.flush();
                    
                    System.out.println("Response sent to " + clientAddress + 
                                     " - Success: " + result.success() + 
                                     ", Rows: " + (result.rows() != null ? result.rows().size() : 0));
                    
                } catch (EOFException | SocketException e) {
                    // Client disconnected normally
                    System.out.println("Client disconnected: " + clientAddress);
                    break;
                } catch (ClassNotFoundException e) {
                    System.err.println("Invalid object received from client: " + e.getMessage());
                    break;
                } catch (IOException e) {
                    System.err.println("IO error with client " + clientAddress + ": " + e.getMessage());
                    break;
                }
            }
            
        } catch (IOException e) {
            System.err.println("Failed to setup client connection: " + e.getMessage());
        } finally {
            cleanup();
        }
    }
    
    /**
     * Cleanup resources untuk client connection
     */
    private void cleanup() {
        try {
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing input stream: " + e.getMessage());
        }
        
        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing output stream: " + e.getMessage());
        }
        
        try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing client socket: " + e.getMessage());
        }
        
        System.out.println("Client connection cleaned up: " + clientSocket.getInetAddress());
    }
}