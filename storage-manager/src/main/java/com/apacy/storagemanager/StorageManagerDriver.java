package com.apacy.storagemanager;

import com.apacy.common.dto.*;
import com.apacy.common.dto.ast.expression.*;
import com.apacy.common.dto.ast.where.*;
import com.apacy.common.enums.DataType;
import com.apacy.common.enums.IndexType;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StorageManagerDriver {

    private static final String DATA_DIR = "interactive_data";
    private static StorageManager sm;
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("   mDBMS-Apacy: Interactive Storage Manager CLI");
        System.out.println("=================================================");
        System.out.println("Ketik 'help' untuk melihat daftar perintah.\n");

        // 1. Inisialisasi
        cleanDataDir();
        sm = new StorageManager(DATA_DIR);
        sm.initialize();
        System.out.println("Storage Manager initialized at './" + DATA_DIR + "'");

        boolean running = true;
        while (running) {
            System.out.print("\nSM-CLI> ");
            String input = scanner.nextLine().trim();
            if (input.isEmpty()) continue;

            String[] tokens = input.split("\\s+");
            String command = tokens[0].toLowerCase();

            try {
                switch (command) {
                    case "setup" -> handleSetupDemo();
                    case "create_table" -> handleCreateTable(tokens);
                    case "insert" -> handleInsert(tokens);
                    case "select" -> handleSelect(input); // Parsing khusus
                    case "update" -> handleUpdate(input); // Parsing khusus
                    case "delete" -> handleDelete(input); // Parsing khusus
                    case "stats" -> handleStats();
                    case "schema" -> handleShowSchema(tokens);
                    case "drop_index" -> handleDropIndex(tokens);
                    case "help" -> printHelp();
                    case "exit", "quit" -> running = false;
                    default -> System.out.println("Perintah tidak dikenal. Ketik 'help'.");
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }

        sm.shutdown();
        System.out.println("Storage Manager shutdown. Goodbye!");
    }

    // ==================================================================================
    // HANDLERS
    // ==================================================================================

    private static void handleSetupDemo() throws IOException {
        System.out.println("Menyiapkan tabel demo 'mahasiswa'...");
        
        List<Column> cols = List.of(
            new Column("nim", DataType.INTEGER),
            new Column("nama", DataType.VARCHAR, 100),
            new Column("ipk", DataType.FLOAT),
            new Column("prodi", DataType.VARCHAR, 10)
        );
        List<IndexSchema> idxs = List.of(
            new IndexSchema("idx_nim", "nim", IndexType.Hash, "mhs_nim.idx"),
            new IndexSchema("idx_ipk", "ipk", IndexType.BPlusTree, "mhs_ipk.idx")
        );
        Schema schema = new Schema("mahasiswa", "mahasiswa.dat", cols, idxs, new ArrayList<>());
        sm.createTable(schema);

        // Populate Data
        insertMhs(13523001, "Asep", 3.5f, "IF");
        insertMhs(13523002, "Budi", 2.9f, "IF");
        insertMhs(13523003, "Charlie", 3.9f, "STI");
        insertMhs(13523004, "Dina", 3.2f, "IF");
        insertMhs(13523005, "Eko", 1.5f, "EL");
        
        System.out.println("Tabel 'mahasiswa' siap dengan 5 data awal.");
    }

    private static void handleInsert(String[] tokens) {
        if (tokens.length < 3) {
            System.out.println("Usage: insert <table_name> <col1=val1> <col2=val2> ...");
            return;
        }
        String tableName = tokens[1];
        Map<String, Object> data = new HashMap<>();

        for (int i = 2; i < tokens.length; i++) {
            String[] pair = tokens[i].split("=");
            if (pair.length != 2) continue;
            data.put(pair[0], parseValue(pair[1]));
        }

        Row row = new Row(data);
        int affected = sm.writeBlock(new DataWrite(tableName, row, null));
        System.out.println("Inserted " + affected + " row(s).");
    }

    // Format: select <table_name> [where col op val]
    private static void handleSelect(String input) {
        Pattern p = Pattern.compile("select\\s+(\\w+)(?:\\s+where\\s+(\\w+)\\s*([<>=!]+)\\s*(.+))?", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(input);

        if (!m.find()) {
            System.out.println("Usage: select <table_name> [where <col> <op> <val>]");
            return;
        }

        String tableName = m.group(1);
        String col = m.group(2);
        String op = m.group(3);
        String valStr = m.group(4);

        WhereConditionNode condition = null;
        boolean useIndex = false;

        if (col != null && op != null && valStr != null) {
            Object val = parseValue(valStr.trim());
            condition = buildComparison(col, op, val);
            // Secara default kita aktifkan index jika ada kondisi
            useIndex = true; 
            System.out.println("Executing Filtered Scan (Index enabled)...");
        } else {
            System.out.println("Executing Full Table Scan...");
        }

        DataRetrieval req = new DataRetrieval(tableName, List.of("*"), condition, useIndex);
        List<Row> rows = sm.readBlock(req);
        printTable(rows);
    }

    // Format: update <table_name> set <col>=<val> [where <col> <op> <val>]
    private static void handleUpdate(String input) {
        // Regex: update (\w+) set (\w+)=(.+?)(?: where (\w+) ([<>=!]+) (.+))?
        Pattern p = Pattern.compile("update\\s+(\\w+)\\s+set\\s+(\\w+)=(.+?)(?:\\s+where\\s+(\\w+)\\s*([<>=!]+)\\s*(.+))?", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(input);

        if (!m.find()) {
            System.out.println("Usage: update <table_name> set <col>=<val> [where <col> <op> <val>]");
            return;
        }

        String tableName = m.group(1);
        String setCol = m.group(2);
        Object setVal = parseValue(m.group(3).trim());
        
        Row updateRow = new Row(Map.of(setCol, setVal));
        WhereConditionNode condition = null;

        if (m.group(4) != null) {
            String whereCol = m.group(4);
            String op = m.group(5);
            Object whereVal = parseValue(m.group(6).trim());
            condition = buildComparison(whereCol, op, whereVal);
        }

        int affected = sm.updateBlock(new DataUpdate(tableName, updateRow, condition));
        System.out.println("Updated " + affected + " row(s).");
    }

    // Format: delete <table_name> [where <col> <op> <val>]
    private static void handleDelete(String input) {
        Pattern p = Pattern.compile("delete\\s+(\\w+)(?:\\s+where\\s+(\\w+)\\s*([<>=!]+)\\s*(.+))?", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(input);

        if (!m.find()) {
            System.out.println("Usage: delete <table_name> [where <col> <op> <val>]");
            return;
        }

        String tableName = m.group(1);
        WhereConditionNode condition = null;

        if (m.group(2) != null) {
            String col = m.group(2);
            String op = m.group(3);
            Object val = parseValue(m.group(4).trim());
            condition = buildComparison(col, op, val);
        }

        int affected = sm.deleteBlock(new DataDeletion(tableName, condition));
        System.out.println("Deleted " + affected + " row(s).");
    }

    private static void handleStats() {
        System.out.println("\n--- Database Statistics ---");
        Map<String, Statistic> stats = sm.getAllStats();
        if (stats.isEmpty()) {
            System.out.println("(No stats available)");
        }
        for (var entry : stats.entrySet()) {
            System.out.println("Table: " + entry.getKey());
            Statistic s = entry.getValue();
            System.out.println("  Rows: " + s.nr() + " | Blocks: " + s.br() + " | RowSize: " + s.lr());
            System.out.println("  Distinct Values: " + s.V());
            System.out.println("  Indexes: " + s.indexedColumn());
            System.out.println("  Min: " + s.minVal());
            System.out.println("  Max: " + s.maxVal());
            System.out.println();
        }
    }

    private static void handleShowSchema(String[] tokens) {
        if (tokens.length < 2) {
            System.out.println("Usage: schema <table_name>");
            return;
        }
        String tableName = tokens[1];
        Schema s = sm.getSchema(tableName);
        if (s == null) {
            System.out.println("Table not found.");
            return;
        }
        System.out.println("Schema for '" + tableName + "':");
        System.out.println("  Columns:");
        s.columns().forEach(c -> System.out.println("    - " + c.name() + " (" + c.type() + ")"));
        System.out.println("  Indexes:");
        s.indexes().forEach(i -> System.out.println("    - " + i.indexName() + " on " + i.columnName() + " (" + i.indexType() + ")"));
    }

    private static void handleDropIndex(String[] tokens) {
        if (tokens.length < 3) {
            System.out.println("Usage: drop_index <table_name> <index_name>");
            return;
        }
        sm.dropIndex(tokens[1], tokens[2]);
    }

    // ==================================================================================
    // UTILS
    // ==================================================================================

    private static void insertMhs(int nim, String nama, float ipk, String prodi) {
        Row row = new Row(Map.of("nim", nim, "nama", nama, "ipk", ipk, "prodi", prodi));
        sm.writeBlock(new DataWrite("mahasiswa", row, null));
    }

    private static WhereConditionNode buildComparison(String colName, String op, Object val) {
        FactorNode colFactor = new ColumnFactor(colName);
        TermNode colTerm = new TermNode(colFactor, List.of());
        ExpressionNode leftExpr = new ExpressionNode(colTerm, List.of());

        FactorNode valFactor = new LiteralFactor(val);
        TermNode valTerm = new TermNode(valFactor, List.of());
        ExpressionNode rightExpr = new ExpressionNode(valTerm, List.of());

        return new ComparisonConditionNode(leftExpr, op, rightExpr);
    }

    private static Object parseValue(String val) {
        // Coba Integer
        try { return Integer.parseInt(val); } catch (NumberFormatException e) {}
        // Coba Float
        try { 
            if (val.contains(".")) return Float.parseFloat(val); 
        } catch (NumberFormatException e) {}
        // Return String
        return val.replace("'", "").replace("\"", "");
    }

    private static void printTable(List<Row> rows) {
        if (rows == null || rows.isEmpty()) {
            System.out.println(">> (0 rows)");
            return;
        }
        System.out.println(">> Found " + rows.size() + " row(s):");
        for (Row r : rows) {
            System.out.println("   " + r.data());
        }
    }

    private static void printHelp() {
        System.out.println("\nAvailable Commands:");
        System.out.println("  setup                                     -> Buat tabel demo & isi data dummy");
        System.out.println("  schema <table_name>                       -> Lihat struktur tabel");
        System.out.println("  insert <table> <col1=val1> <col2=val2>    -> Insert data");
        System.out.println("  select <table> [where col op val]         -> Read data (auto index selection)");
        System.out.println("  update <table> set <c=v> [where c op v]   -> Update data");
        System.out.println("  delete <table> [where col op val]         -> Delete data");
        System.out.println("  stats                                     -> Lihat statistik database");
        System.out.println("  drop_index <table> <index_name>           -> Hapus indeks");
        System.out.println("  exit                                      -> Keluar");
    }

    private static void handleCreateTable(String[] tokens) {
        System.out.println("Fitur create_table manual disederhanakan. Gunakan 'setup' untuk demo cepat.");
        // (Bisa dikembangkan jika perlu, tapi setup() biasanya cukup untuk demo)
    }

    private static void cleanDataDir() {
        File dir = new File(DATA_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) for (File f : files) f.delete();
            dir.delete();
        }
    }
}