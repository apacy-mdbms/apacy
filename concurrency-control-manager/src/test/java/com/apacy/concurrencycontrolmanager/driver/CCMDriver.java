package com.apacy.concurrencycontrolmanager.driver;

import com.apacy.common.dto.Response;
import com.apacy.common.enums.Action;
import com.apacy.concurrencycontrolmanager.ConcurrencyControlManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// --- Helper Classes ---

/**
 * Representasi Global Database.
 */
class SimulatedDatabase {
    private final ConcurrentHashMap<String, String> dataStore = new ConcurrentHashMap<>();

    // MONITOR GLOBAL untuk Retry Logic
    // Setiap kali ada yang commit, kita notify thread yang menunggu retry.
    public final Object commitMonitor = new Object();

    public SimulatedDatabase() {
        dataStore.put("A", "A_Init");
        dataStore.put("B", "B_Init");
        dataStore.put("C", "C_Init");
    }

    public String readGlobal(String key) {
        return dataStore.getOrDefault(key, "NULL");
    }

    public synchronized void applyCommit(String txName, Map<String, String> changes) {
        if (changes.isEmpty()) return;
        System.out.println("   -> Applying " + changes.size() + " writes from " + txName + " to Global DB.");
        dataStore.putAll(changes);
    }

    public synchronized void printSnapshot(String context) {
        System.out.println("\n--- DB SNAPSHOT [" + context + "] ---");
        new TreeMap<>(dataStore).forEach((k, v) -> 
            System.out.println("   " + k + ": " + v)
        );
        System.out.println("----------------------------------------\n");
    }
}

class SimOperation {
    enum Type { READ, WRITE, SLEEP, COMMIT, ROLLBACK }
    Type type; String key; String value; long duration; 

    public SimOperation(Type type, String key, String value, long duration) {
        this.type = type; this.key = key; this.value = value; this.duration = duration;
    }
}

class SimTransactionDef {
    String name;
    long startDelay;
    List<SimOperation> operations = new ArrayList<>();

    public SimTransactionDef(String name, long startDelay) {
        this.name = name; this.startDelay = startDelay;
    }
}

// --- Main Worker Thread ---

class TransactionTask implements Runnable {
    private final ConcurrencyControlManager ccm;
    private final SimulatedDatabase db;
    private final SimTransactionDef def;
    private final Map<String, String> localWriteBuffer = new HashMap<>();

    public TransactionTask(ConcurrencyControlManager ccm, SimulatedDatabase db, SimTransactionDef def) {
        this.ccm = ccm; this.db = db; this.def = def;
    }

    private void log(String msg) {
        System.out.printf("[%tT] [%s] %s%n", System.currentTimeMillis(), def.name, msg);
    }

    private void simulateProcessing(long ms) {
        if (ms > 0) {
            try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
        }
    }

    @Override
    public void run() {
        // Initial start delay (hanya sekali di awal)
        if (def.startDelay > 0) {
            try { Thread.sleep(def.startDelay); } catch (InterruptedException e) { return; }
        }

        boolean committed = false;
        int retryCount = 0;

        // --- RETRY LOOP ---
        while (!committed && retryCount < 5) { // Max 5x retry untuk safety
            
            if (retryCount > 0) {
                log("--- RETRY ATTEMPT #" + retryCount + " ---");
            }

            localWriteBuffer.clear(); // Reset buffer lokal setiap retry
            int txId = ccm.beginTransaction();
            log("STARTED (CCM ID: " + txId + ")");

            boolean aborted = false;
            boolean shouldRetry = false; // Flag khusus apakah ini abort yang perlu retry

            try {
                // Execute Operations
                for (SimOperation op : def.operations) {
                    if (aborted) break;

                    switch (op.type) {
                        case SLEEP -> {
                            log("Sleeping " + op.duration + "ms...");
                            simulateProcessing(op.duration);
                        }
                        case READ -> {
                            log("Requesting READ " + op.key);
                            if (validateWithRetry(txId, op.key, Action.READ)) {
                                String val = localWriteBuffer.containsKey(op.key) 
                                    ? localWriteBuffer.get(op.key) + " (Local)" 
                                    : db.readGlobal(op.key) + " (Global)";
                                log("READ SUCCESS: " + op.key + " = " + val);
                                simulateProcessing(op.duration);
                            } else {
                                log("ABORTED during READ " + op.key);
                                aborted = true; shouldRetry = true;
                            }
                        }
                        case WRITE -> {
                            log("Requesting WRITE " + op.key);
                            if (validateWithRetry(txId, op.key, Action.WRITE)) {
                                localWriteBuffer.put(op.key, op.value);
                                log("WRITE BUFFERED: " + op.key);
                                simulateProcessing(op.duration);
                            } else {
                                log("ABORTED during WRITE " + op.key);
                                aborted = true; shouldRetry = true;
                            }
                        }
                        case COMMIT -> {
                            log("Commit Requested...");
                            ccm.endTransaction(txId, true);
                            db.applyCommit(def.name, localWriteBuffer);
                            log("COMMITTED Successfully.");
                            db.printSnapshot("After COMMIT by " + def.name);
                            committed = true;
                            
                            // NOTIFY RETRY WAITERS
                            synchronized (db.commitMonitor) {
                                db.commitMonitor.notifyAll();
                            }
                        }
                        case ROLLBACK -> {
                            log("ROLLBACK Requested (User Logic)...");
                            ccm.endTransaction(txId, false);
                            log("ROLLED BACK. No Retry for User Logic Rollback.");
                            return; // Jika user sengaja rollback, jangan retry (asumsi logika bisnis)
                        }
                    }
                    if (committed) break;
                }

                // Handle Auto-Commit jika loop selesai tanpa error
                if (!aborted && !committed) {
                    log("Auto-Committing...");
                    ccm.endTransaction(txId, true);
                    db.applyCommit(def.name, localWriteBuffer);
                    log("COMMITTED.");
                    db.printSnapshot("After Auto-COMMIT by " + def.name);
                    committed = true;
                    
                    synchronized (db.commitMonitor) {
                        db.commitMonitor.notifyAll();
                    }
                }

            } catch (Exception e) {
                log("EXCEPTION: " + e.getMessage());
                try { ccm.endTransaction(txId, false); } catch (Exception ignored) {}
                aborted = true;
                shouldRetry = true;
            }

            // --- CLEANUP & WAIT LOGIC ---
            if (aborted) {
                log("Cleaning up resources due to ABORT...");
                try { ccm.endTransaction(txId, false); } catch (Exception ignored) {}
                
                if (shouldRetry) {
                    retryCount++;
                    log("Transaction Failed. Waiting for other transaction to COMMIT before retrying...");
                    
                    synchronized (db.commitMonitor) {
                        try {
                            // Tunggu sampai ada yang notifyAll() ATAU timeout 5 detik supaya tidak deadlock selamanya
                            db.commitMonitor.wait(5000); 
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    log("Woke up! Retrying transaction...");
                } else {
                    break; // Abort fatal, stop.
                }
            }
        }
        
        if (!committed) {
            log("FAILED after " + retryCount + " retries.");
        }
    }

    private boolean validateWithRetry(int txId, String key, Action action) {
        // Simple spin-wait logic for Lock Based Protocol
        int waitAttempts = 0;
        while (true) {
            Response response = ccm.validateObject(key, txId, action);
            if (response.isAllowed()) return true;

            String reason = response.reason();
            if (reason != null && (reason.toUpperCase().contains("ABORT") || reason.toUpperCase().contains("WOUNDED"))) {
                return false;
            }

            if (waitAttempts++ > 10) return false; // Timeout lokal untuk lock wait (bukan retry tx)
            
            log("WAITING for " + key + " (" + action + ")...");
            try { Thread.sleep(500); } catch (InterruptedException e) { return false; }
        }
    }
}

// --- Driver Main Class ---

public class CCMDriver {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("=== CCM DRIVER (Retry on Abort) ===");

        // Setup Algorithm (Sama seperti sebelumnya)
        System.out.println("1. Lock (Default)\n2. Timestamp\n3. Validation");
        System.out.print("Choice: ");
        String algo = "lock";
        String choice = scanner.nextLine();
        if(choice.equals("2")) algo = "timestamp";
        if(choice.equals("3")) algo = "validation";

        ConcurrencyControlManager ccm = new ConcurrencyControlManager(algo);
        try { ccm.initialize(); } catch (Exception e) { e.printStackTrace(); scanner.close(); return; }

        // Setup Scenario
        System.out.print("Scenario file: ");
        String filePath = scanner.nextLine();
        List<SimTransactionDef> transactions = loadScenario(filePath);
        if (transactions.isEmpty()) {
            scanner.close();
            return;
        }

        SimulatedDatabase db = new SimulatedDatabase();
        
        System.out.println("Starting Simulation...");
        ExecutorService executor = Executors.newFixedThreadPool(transactions.size());
        
        for (SimTransactionDef txDef : transactions) {
            executor.submit(new TransactionTask(ccm, db, txDef));
        }

        executor.shutdown();
        try { executor.awaitTermination(60, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        
        db.printSnapshot("FINAL STATE");
        ccm.shutdown();
        scanner.close();
    }

    // Load Scenario Logic (Sama persis seperti sebelumnya, disalin untuk kelengkapan)
    private static List<SimTransactionDef> loadScenario(String filePath) {
        List<SimTransactionDef> list = new ArrayList<>();
        SimTransactionDef currentTx = null;
        try (Scanner s = new Scanner(new File(filePath))) {
            while (s.hasNextLine()) {
                String line = s.nextLine().trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split("\\s+");
                String cmd = parts[0].toUpperCase();
                
                switch (cmd) {
                    case "BEGIN_TX" -> {
                        String name = parts.length > 1 ? parts[1] : "TX-" + (list.size()+1);
                        long delay = parts.length > 2 ? Long.parseLong(parts[2]) : 0;
                        currentTx = new SimTransactionDef(name, delay);
                    }
                    case "END_TX" -> { if(currentTx!=null) { list.add(currentTx); currentTx=null; } }
                    case "WRITE" -> {
                         if(currentTx!=null) {
                            // Parsing logic sederhana
                            int q1 = line.indexOf('"'), q2 = line.lastIndexOf('"');
                            if (q1 != -1 && q2 > q1) {
                                String key = line.substring(line.indexOf(' '), q1).trim();
                                String val = line.substring(q1+1, q2);
                                String dur = line.substring(q2+1).trim();
                                currentTx.operations.add(new SimOperation(SimOperation.Type.WRITE, key, val, dur.isEmpty()?0:Long.parseLong(dur)));
                            } else {
                                currentTx.operations.add(new SimOperation(SimOperation.Type.WRITE, parts[1], parts[2], parts.length>3?Long.parseLong(parts[3]):0));
                            }
                         }
                    }
                    case "READ" -> { if(currentTx!=null) currentTx.operations.add(new SimOperation(SimOperation.Type.READ, parts[1], null, parts.length>2?Long.parseLong(parts[2]):0)); }
                    case "SLEEP" -> { if(currentTx!=null) currentTx.operations.add(new SimOperation(SimOperation.Type.SLEEP, null, null, Long.parseLong(parts[1]))); }
                    case "COMMIT" -> { if(currentTx!=null) currentTx.operations.add(new SimOperation(SimOperation.Type.COMMIT, null, null, 0)); }
                    case "ROLLBACK" -> { if(currentTx!=null) currentTx.operations.add(new SimOperation(SimOperation.Type.ROLLBACK, null, null, 0)); }
                }
            }
        } catch (FileNotFoundException e) { System.err.println("File not found"); }
        return list;
    }
}