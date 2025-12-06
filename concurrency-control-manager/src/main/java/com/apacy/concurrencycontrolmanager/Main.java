package com.apacy.concurrencycontrolmanager;

import com.apacy.common.dto.Response;
import com.apacy.common.enums.Action;

import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws Exception {

        String algorithm = "lock"; // Default

        System.out.println("=================================");
        System.out.println("           [CCM DRIVER]");
        System.out.println("         Algorithm = " + algorithm);
        System.out.println("=================================");

        ConcurrencyControlManager ccm = new ConcurrencyControlManager(algorithm);
        ccm.initialize();
        
        // Command buat ngetes BEGIN, READ <txId> <objectId>, WRITE <txId> <objectId>, COMMIT <txId>, ABORT <txId>, EXIT
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.print(">> ");
            String line = reader.readLine();

            if (line == null || line.isBlank()) continue;

            String[] input = line.trim().split(" ");
            String command = input[0].toUpperCase();

            try {
                switch (command) {

                    case "BEGIN" -> {
                        int tx = ccm.beginTransaction();
                        System.out.println("TX" + tx + " BEGIN");
                    }

                    case "READ", "WRITE" -> {
                        if (input.length < 3) {
                            System.out.println("Format: " + command + " <txId> <objectId>");
                            break;
                        }

                        int txId = Integer.parseInt(input[1]);
                        String objectId = input[2];
                        Action action = Action.valueOf(command);

                        Response r = ccm.validateObjects(List.of(objectId), txId, action);

                        printResult(txId, command, objectId, r);
                    }

                    case "COMMIT" -> {
                        int txId = Integer.parseInt(input[1]);
                        ccm.endTransaction(txId, true);
                        System.out.println("TX" + txId + " COMMIT");
                    }

                    case "ABORT" -> {
                        int txId = Integer.parseInt(input[1]);
                        ccm.endTransaction(txId, false);
                        System.out.println("TX" + txId + " ABORT");
                    }

                    case "EXIT" -> {
                        ccm.shutdown();
                        System.out.println("[CCM] Shutdown complete");
                        return;
                    }

                    default -> System.out.println("Unknown command");
                }

            } catch (Exception e) {
                System.out.println("ERROR: " + e.getMessage());
            }
        }
    }

    private static void printResult(int txId, String action, String object, Response r) {
        System.out.println("TX" + txId + " " + action + " " + object + " -> " +(r != null && r.isAllowed() ? "SUCCESS" : "FAILED")
        );
    }
}
