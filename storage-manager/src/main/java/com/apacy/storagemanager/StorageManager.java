package com.apacy.storagemanager;

import com.apacy.common.DBMSComponent;
import com.apacy.common.dto.Column;
import com.apacy.common.dto.DataDeletion;
import com.apacy.common.dto.DataRetrieval;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ForeignKeySchema;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.Statistic;
import com.apacy.common.dto.ast.where.*;
import com.apacy.common.dto.ast.expression.*;
import com.apacy.common.dto.DataUpdate;
import com.apacy.common.enums.DataType;
import com.apacy.common.enums.IndexType;
import com.apacy.common.interfaces.IStorageManager;
import com.apacy.storagemanager.index.*;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.stream.Collectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class StorageManager extends DBMSComponent implements IStorageManager {

  private static class ParsedFilter {
    String columnName;
    String operator;
    Object value;

    ParsedFilter(String columnName, String operator, Object value) {
      this.columnName = columnName;
      this.operator = operator;
      this.value = value;
    }
  }

  private final BlockManager blockManager;
  private final Serializer serializer;
  private final StatsCollector statsCollector;
  private final CatalogManager catalogManager;
  private final IndexManager indexManager; // Helper class untuk B+Tree/Hash

  public StorageManager(String dataDirectory) {
    super("Storage Manager");
    this.catalogManager = new CatalogManager(dataDirectory + "/system_catalog.dat");
    this.blockManager = new BlockManager(dataDirectory);
    this.serializer = new Serializer(this.catalogManager);
    this.statsCollector = new StatsCollector(this.catalogManager, this.blockManager, this.serializer);
    this.indexManager = new IndexManager();
  }

  @Override
  public void initialize() {
    System.out.println("DEBUG: StorageManager.initialize() started");
    try {
      this.catalogManager.loadCatalog();
      System.out.println("DEBUG: Catalog loaded successfully");

      for (Schema schema : catalogManager.getAllSchemas()) {
        System.out.println("Processing schema: " + schema.tableName());
        for (IndexSchema idx : schema.indexes()) {
          System.out.println("Creating index: " + idx.indexName() + " for column: " + idx.columnName());
          try {
            IIndex<?, ?> index = createIndexInstance(schema, idx);
            indexManager.register(schema.tableName(), idx.columnName(), idx.indexType().toString(), index);
            System.out.println("Successfully created index: " + idx.indexName());
          } catch (Exception indexError) {
            System.err.println("Error creating index " + idx.indexName() + ": " + indexError.getMessage());
            indexError.printStackTrace();
          }
        }
      }
      this.indexManager.loadAll(this.catalogManager);
    } catch (Exception e) {
      System.err.println("Gagal menginitialize Storage Manager! " + e.getMessage());
    }
  }

  @Override
  public void shutdown() {

    indexManager.flushAll(this.catalogManager);

  }

  public CatalogManager getCatalogManager() {
    return this.catalogManager;
  }

  @Override
  public Schema getSchema(String tableName) {
    return this.catalogManager.getSchema(tableName);
  }

  @Override
  public List<Row> readBlock(DataRetrieval dataRetrieval) {
    try {
      Schema schema = catalogManager.getSchema(dataRetrieval.tableName());
      if (schema == null) {
        throw new IOException("Tabel tidak ditemukan di katalog: " + dataRetrieval.tableName());
      }
      String fileName = schema.dataFile();
      List<Row> allRows = new ArrayList<>();
      Object filterRoot = dataRetrieval.filterCondition();

      List<ParsedFilter> parsedFilters = parseFilterCondition(filterRoot, schema);
      boolean isStringFilter = !(filterRoot instanceof WhereConditionNode) && filterRoot != null;

      if (dataRetrieval.useIndex() && !parsedFilters.isEmpty()) {
        for (IndexSchema idxSchema : schema.indexes()) {
          IIndex index = indexManager.get(
              schema.tableName(),
              idxSchema.columnName(),
              idxSchema.indexType().toString());

          if (index == null)
            continue;

          List<Integer> ridList = Collections.emptyList();
          boolean indexUsed = false;

          Object eqValue = null;
          for (ParsedFilter pf : parsedFilters) {
            if (pf.columnName.equals(idxSchema.columnName()) && "=".equals(pf.operator)) {
              eqValue = pf.value;
              break;
            }
          }

          if (eqValue != null) {
            // Equality Lookup
            ridList = index.getAddress(eqValue);
            indexUsed = true;
          }
          // B. Jika tidak ada Equality, coba Range Scan (Hanya BTree)
          else if (idxSchema.indexType() == IndexType.BPlusTree) {
            // Cari batas bawah (>=, >) dan batas atas (<=, <)
            Object minVal = null;
            boolean minInclusive = true;
            Object maxVal = null;
            boolean maxInclusive = true;

            for (ParsedFilter pf : parsedFilters) {
              if (!pf.columnName.equals(idxSchema.columnName()))
                continue;

              if (">=".equals(pf.operator)) {
                minVal = pf.value;
                minInclusive = true;
              } else if (">".equals(pf.operator)) {
                minVal = pf.value;
                minInclusive = false;
              } else if ("<=".equals(pf.operator)) {
                maxVal = pf.value;
                maxInclusive = true;
              } else if ("<".equals(pf.operator)) {
                maxVal = pf.value;
                maxInclusive = false;
              }
            }

            // Jika ada salah satu bound (min/max), lakukan range scan
            if (minVal != null || maxVal != null) {
              // Asumsi method di interface IIndex / BPlusTree: getAddresses(min, minIncl,
              // max, maxIncl)
              // Jika index generik, perlu cast ke BPlusIndex atau interface yang support
              // range
              if (index instanceof BPlusIndex) {
                ridList = ((BPlusIndex) index).getAddresses((Comparable) minVal, minInclusive, (Comparable) maxVal,
                    maxInclusive);
                indexUsed = true;
              }
            }
          }

          // Jika index berhasil digunakan (Equality atau Range), ambil datanya
          if (indexUsed) {
            List<Row> resultRows = new ArrayList<>();
            for (int encodedRid : ridList) {
              long blockNo = (encodedRid >>> 16);
              int slotNo = encodedRid & 0xFFFF;

              byte[] block = blockManager.readBlock(schema.dataFile(), blockNo);
              Row r = serializer.readRowAtSlot(block, schema, slotNo);

              if (r != null) {
                // Validasi ulang row dengan semua filter
                boolean matches = isStringFilter ? rowMatchesFilters(r, parsedFilters)
                    : evaluateCondition(r, filterRoot);

                if (matches) {
                  resultRows.add(projectColumns(r, dataRetrieval.columns()));
                }
              }
            }
            return resultRows;
          }
        }
      }

      long blockCount = blockManager.getBlockCount(fileName);

      for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
        byte[] blockData = blockManager.readBlock(fileName, blockNumber);
        List<Row> rowsOfBlock = serializer.deserializeBlock(blockData, schema);
        allRows.addAll(rowsOfBlock);
      }

      return allRows.stream()
          .filter(
              (Row row) -> isStringFilter ? rowMatchesFilters(row, parsedFilters) : evaluateCondition(row, filterRoot))
          .map((Row row) -> projectColumns(row, dataRetrieval.columns()))
          .collect(Collectors.toList());

    } catch (IOException e) {
      System.err.println("Error reading block: " + e.getMessage());
      return Collections.emptyList();
    }
  }

  // ==================================================================================
  // [AST EVALUATOR] LOGIKA UTAMA FILTERING
  // ==================================================================================

  /**
   * Mengevaluasi apakah Row memenuhi kondisi filter (AST).
   * Mendukung: AND, OR, =, >, <, >=, <=, !=
   */
  private boolean evaluateCondition(Row row, Object condition) {
    // Jika filter null, berarti "SELECT *", semua lolos.
    if (condition == null) {
      return true;
    }

    // 1. Handle Logika Binary (AND / OR)
    if (condition instanceof BinaryConditionNode bin) {
      boolean leftResult = evaluateCondition(row, bin.left());
      boolean rightResult = evaluateCondition(row, bin.right());

      if ("AND".equalsIgnoreCase(bin.operator())) {
        return leftResult && rightResult;
      } else if ("OR".equalsIgnoreCase(bin.operator())) {
        return leftResult || rightResult;
      }
    }

    // 2. Handle Perbandingan (=, >, <, dll)
    if (condition instanceof ComparisonConditionNode comp) {
      Object leftVal = extractValue(row, comp.leftOperand());
      Object rightVal = extractValue(row, comp.rightOperand());
      return compareValues(leftVal, rightVal, comp.operator());
    }

    // 3. Handle Unary (NOT) - Jika ada di DTO common
    if (condition instanceof UnaryConditionNode unary) {
      if ("NOT".equalsIgnoreCase(unary.operator())) {
        return !evaluateCondition(row, unary.operand());
      }
    }

    return false;
  }

  /**
   * Mengambil nilai real dari ExpressionNode (bisa dari Kolom Row atau Literal).
   */
  private Object extractValue(Row row, ExpressionNode expr) {
    if (expr == null || expr.term() == null || expr.term().factor() == null)
      return null;

    FactorNode factor = expr.term().factor();

    if (factor instanceof ColumnFactor col) {
      // Ambil value dari map data Row berdasarkan nama kolom
      return row.get(col.columnName());
    } else if (factor instanceof LiteralFactor lit) {
      // FIX: Tidak menggunakan lit.type(), kirim null sebagai type
      String valStr = (lit.value() != null) ? lit.value().toString() : null;
      return parseLiteral(valStr, null);
    }
    return null;
  }

  /**
   * Konversi String literal ke Object Java yang sesuai.
   */
  private Object parseLiteral(String value, String type) {
    if (value == null)
      return null;
    String clean = value.replace("'", "").replace("\"", ""); // Hapus quote

    // Jika type tidak diberikan (null), coba tebak sendiri
    if (type == null) {
      try {
        // Cek Integer
        if (value.matches("-?\\d+")) {
          return Integer.parseInt(value);
        }
        // Cek Double/Float
        else if (value.matches("-?\\d+\\.\\d+")) {
          return Double.parseDouble(value);
        }
      } catch (NumberFormatException e) {
        // Jika gagal parsing angka, anggap sebagai String biasa
      }
    }
    // Jika type diberikan (opsional, untuk masa depan)
    else if ("INTEGER".equalsIgnoreCase(type)) {
      try {
        return Integer.parseInt(value);
      } catch (Exception e) {
      }
    }

    return clean;
  }

  /**
   * Membandingkan dua nilai object dengan operator tertentu.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private boolean compareValues(Object v1, Object v2, String operator) {
    if (v1 == null || v2 == null) {
      // Khusus != null logic jika diperlukan, tapi standar SQL null != anything is
      // unknown (false)
      if ("!=".equals(operator) || "<>".equals(operator)) {
        return v1 != v2; // Simple check
      }
      return false;
    }

    // Penanganan beda tipe (misal Integer vs Double, atau Integer vs String angka)
    if (v1 instanceof Number && v2 instanceof String) {
      try {
        v2 = Double.parseDouble((String) v2);
      } catch (Exception e) {
      }
    }
    if (v1 instanceof String && v2 instanceof Number) {
      try {
        v1 = Double.parseDouble((String) v1);
      } catch (Exception e) {
      }
    }

    // Convert ke Double untuk perbandingan angka aman
    if (v1 instanceof Number && v2 instanceof Number) {
      double d1 = ((Number) v1).doubleValue();
      double d2 = ((Number) v2).doubleValue();
      int comparison = Double.compare(d1, d2);
      return checkOp(comparison, operator);
    }

    // Default String comparison
    if (v1 instanceof Comparable && v2 instanceof Comparable) {
      try {
        int comparison = ((Comparable) v1).compareTo((Comparable) v2);
        return checkOp(comparison, operator);
      } catch (ClassCastException e) {
        return false; // Tipe data tidak kompatibel
      }
    }

    return false;
  }

  private boolean checkOp(int comparison, String operator) {
    return switch (operator) {
      case "=" -> comparison == 0;
      case ">" -> comparison > 0;
      case "<" -> comparison < 0;
      case ">=" -> comparison >= 0;
      case "<=" -> comparison <= 0;
      case "!=", "<>" -> comparison != 0;
      default -> false;
    };
  }

  private String getColumnNameSafe(ExpressionNode expr) {
    if (expr != null && expr.term() != null && expr.term().factor() instanceof ColumnFactor col) {
      return col.columnName();
    }
    return null;
  }

  private Object getLiteralValueSafe(ExpressionNode expr) {
    if (expr != null && expr.term() != null && expr.term().factor() instanceof LiteralFactor lit) {
      // FIX: Tidak menggunakan lit.type()
      String valStr = (lit.value() != null) ? lit.value().toString() : null;
      return parseLiteral(valStr, null);
    }
    return null;
  }

  @Override
  public int writeBlock(DataWrite dataWrite) {
    // TODO:
    // 1. Panggil serializer.serialize(dataWrite.newData())
    // 2. Panggil blockManager.writeBlock(...)
    // 3. Update index
    // 4. Kembalikan jumlah baris
    try {
      Schema schema = catalogManager.getSchema(dataWrite.tableName());
      if (schema == null) {
        throw new IOException("Tabel tidak ditemukan: " + dataWrite.tableName());
      }
      String fileName = schema.dataFile();

      // Global duplicate-row prevention: if an identical row (all columns)
      // already exists in the table, reject the insert.
      try {
        long existingBlocks = blockManager.getBlockCount(fileName);
        for (long b = 0; b < existingBlocks; b++) {
          byte[] blockData = blockManager.readBlock(fileName, b);
          List<Row> rows = serializer.deserializeBlock(blockData, schema);
          for (Row r : rows) {
            if (r != null && r.data().equals(dataWrite.newData().data())) {
              System.err.println("Duplicate row detected. Insert rejected.");
              return 0;
            }
          }
        }
      } catch (Exception dupScanErr) {
        System.err.println("Warning: duplicate scan failed: " + dupScanErr.getMessage());
      }

      long blockCount = blockManager.getBlockCount(fileName);
      long targetBlockNumber = -1;
      int newSlotId = -1;
      boolean packed = false;
      // byte[] blockData;

      for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
        byte[] candidate = blockManager.readBlock(fileName, blockNumber);
        try {
          byte[] updated = serializer.packRowToBlock(candidate, dataWrite.newData(), schema);
          blockManager.writeBlock(fileName, blockNumber, updated);
          targetBlockNumber = blockNumber;
          newSlotId = serializer.getLastPackedSlotId();
          packed = true;
          break;
        } catch (IOException fullBlock) {
          // next block bro
          continue;
        }
      }

      if (!packed) {
        byte[] newBlock = serializer.initializeNewBlock();
        newBlock = serializer.packRowToBlock(newBlock, dataWrite.newData(), schema);
        targetBlockNumber = blockManager.appendBlock(fileName, newBlock);
        newSlotId = serializer.getLastPackedSlotId();
      }

      blockManager.flush();

      for (IndexSchema idxSchema : schema.indexes()) {
        @SuppressWarnings("unchecked")
        IIndex<Object, Integer> index = (IIndex<Object, Integer>) indexManager.get(
            schema.tableName(), idxSchema.columnName(), idxSchema.indexType().toString());

        if (index != null) {
          Object key = dataWrite.newData().data().get(idxSchema.columnName());
          int ridValue = (int) ((targetBlockNumber << 16) | (newSlotId & 0xFFFF));
          index.insertData(key, ridValue);
          index.writeToFile(this.catalogManager);
        }
      }

      return 1;
    } catch (IOException e) {
      System.err.println("Error writing block: " + e.getMessage());
      return 0;
    }
  }

  public void createTable(Schema newSchema) throws IOException {
    System.out.println("StorageManager: Menerima perintah CREATE TABLE untuk: " + newSchema.tableName());
    // 1. Tambahkan skema baru ke cache memori Katalog
    catalogManager.addSchemaToCache(newSchema);

    // 2. Tulis ulang (flush) seluruh katalog ke disk
    catalogManager.writeCatalog();

    // 3. Buat file .dat kosong (dengan 1 blok header)
    byte[] initialBlock = serializer.initializeNewBlock();
    blockManager.writeBlock(newSchema.dataFile(), 0, initialBlock);

    // 4. TODO: Buat file .idx (jika ada indeks)
    for (IndexSchema idxSchema : newSchema.indexes()) {
      IIndex<?, ?> index = createIndexInstance(newSchema, idxSchema);
      indexManager.register(newSchema.tableName(), idxSchema.columnName(), idxSchema.indexType().toString(),
          index);
      index.writeToFile(this.catalogManager); // Tulis file .idx kosong
    }

    System.out.println("StorageManager: Tabel " + newSchema.tableName() + " berhasil dibuat di disk.");
  }

  @Override
  public int deleteBlock(DataDeletion dataDeletion) {
    try {
      Schema schema = catalogManager.getSchema(dataDeletion.tableName());
      if (schema == null) {
        throw new IOException("Table " + dataDeletion.tableName() + " not found");
      }

      String fileName = schema.dataFile();
      Object filterRoot = dataDeletion.filterCondition();

      List<ParsedFilter> parsedFilters = parseFilterCondition(filterRoot, schema);
      boolean isStringFilter = !(filterRoot instanceof WhereConditionNode) && filterRoot != null;

      int deletedRows = 0;
      List<Integer> ridsToDelete = Collections.emptyList();
      boolean useIndexScan = false;

      if (!parsedFilters.isEmpty()) {
        for (IndexSchema idxSchema : schema.indexes()) {
          IIndex index = indexManager.get(
              schema.tableName(),
              idxSchema.columnName(),
              idxSchema.indexType().toString());

          if (index == null)
            continue;

          Object eqValue = null;
          for (ParsedFilter pf : parsedFilters) {
            if (pf.columnName.equals(idxSchema.columnName()) && "=".equals(pf.operator)) {
              eqValue = pf.value;
              break;
            }
          }

          if (eqValue != null) {
            ridsToDelete = index.getAddress(eqValue);
            useIndexScan = true;
          } else if (idxSchema.indexType() == IndexType.BPlusTree) {
            Object minVal = null;
            boolean minInclusive = true;
            Object maxVal = null;
            boolean maxInclusive = true;

            for (ParsedFilter pf : parsedFilters) {
              if (!pf.columnName.equals(idxSchema.columnName()))
                continue;
              if (">=".equals(pf.operator)) {
                minVal = pf.value;
                minInclusive = true;
              } else if (">".equals(pf.operator)) {
                minVal = pf.value;
                minInclusive = false;
              } else if ("<=".equals(pf.operator)) {
                maxVal = pf.value;
                maxInclusive = true;
              } else if ("<".equals(pf.operator)) {
                maxVal = pf.value;
                maxInclusive = false;
              }
            }

            if (minVal != null || maxVal != null) {
              if (index instanceof BPlusIndex) {
                ridsToDelete = ((BPlusIndex) index).getAddresses((Comparable) minVal, minInclusive, (Comparable) maxVal,
                    maxInclusive);
                useIndexScan = true;
              }
            }
          }

          if (useIndexScan)
            break; // Cukup pakai satu index
        }
      }

      // --- 3. EXECUTION ---
      if (useIndexScan) {
        // A. STRATEGI INDEX SCAN
        for (int encodedRid : ridsToDelete) {
          long blockNo = (encodedRid >>> 16);
          int slotNo = encodedRid & 0xFFFF;

          byte[] blockData = blockManager.readBlock(fileName, blockNo);
          Row row = serializer.readRowAtSlot(blockData, schema, slotNo);

          if (row != null) {
            boolean matches = isStringFilter ? rowMatchesFilters(row, parsedFilters)
                : evaluateCondition(row, filterRoot);

            if (matches) {
              if (serializer.deleteSlot(blockData, slotNo)) {
                blockManager.writeBlock(fileName, blockNo, blockData); // Write back immediately
                removeRowFromIndexes(schema, blockNo, slotNo, row);
                deletedRows++;
              }
            }
          }
        }
        blockManager.flush();
      } else {
        // B. STRATEGI FULL TABLE SCAN (Fallback)
        long blockCount = blockManager.getBlockCount(fileName);

        for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
          byte[] blockData = blockManager.readBlock(fileName, blockNumber);
          int slotCount = serializer.getSlotCount(blockData);
          boolean blockDirty = false;

          for (int slotId = 0; slotId < slotCount; slotId++) {
            Row row = serializer.readRowAtSlot(blockData, schema, slotId);

            if (row == null)
              continue;

            boolean matches = isStringFilter ? rowMatchesFilters(row, parsedFilters)
                : evaluateCondition(row, filterRoot);

            if (!matches)
              continue;

            if (serializer.deleteSlot(blockData, slotId)) {
              deletedRows++;
              blockDirty = true;
              removeRowFromIndexes(schema, blockNumber, slotId, row);
            }
          }

          if (blockDirty) {
            blockManager.writeBlock(fileName, blockNumber, blockData);
          }
        }
        blockManager.flush();
      }

      return deletedRows;
    } catch (IOException e) {
      System.err.println("Error deleting block: " + e.getMessage());
      return 0;
    }
  }

  @Override
  public void setIndex(String table, String column, String indexType) {
    try {
      Schema schema = catalogManager.getSchema(table);
      if (schema == null) {
        throw new IOException("Table " + table + " not found");
      }

      Column targetColumn = schema.getColumnByName(column);
      if (targetColumn == null) {
        throw new IOException("Column " + column + " not found on table " + table);
      }

      IndexType typeEnum = findIndexType(indexType);
      String suffix = typeEnum.name().toLowerCase();
      String indexFile = table + "_" + column + "_" + suffix + ".idx";
      String indexName = "idx_" + table + "_" + column + "_" + suffix;

      IndexSchema newIndexSchema = new IndexSchema(indexName, column, typeEnum, indexFile);
      IIndex<?, ?> index = createIndexInstance(schema, newIndexSchema);
      indexManager.register(table, column, typeEnum.toString(), index);

      @SuppressWarnings("unchecked")
      IIndex<Object, Integer> typedIndex = (IIndex<Object, Integer>) index;
      populateIndexFromTable(schema, typedIndex, column);
      index.writeToFile(catalogManager);

      List<IndexSchema> updatedIndexes = new ArrayList<>(schema.indexes());
      updatedIndexes.add(newIndexSchema);
      Schema updatedSchema = new Schema(schema.tableName(), schema.dataFile(), schema.columns(), updatedIndexes);
      catalogManager.updateSchema(updatedSchema);
      catalogManager.writeCatalog();
    } catch (IOException e) {
      System.err.println("Failed to create a new index: " + e.getMessage());
    }
  }

  @Override
  public Map<String, Statistic> getAllStats() {
    // TODO:
    // 1. Panggil statsCollector.collectStats()
    // 2. Kembalikan map dari String, Statistic (Nama tabel dan statistiknya)
    return this.statsCollector.getAllStats();
  }

  /**
   * Apply filter condition ke sebuah row
   * 
   * @param row           as row yang mau difilter
   * @param dataRetrieval as data yang ingin dicek
   * @return true jika lolos filter
   */

  private IIndex<?, ?> createIndexInstance(Schema tableSchema, IndexSchema idxSchema) throws IOException {
    Column col = tableSchema.getColumnByName(idxSchema.columnName());
    if (col == null) {
      throw new IOException("Kolom '" + idxSchema.columnName() + "' untuk indeks tidak ditemukan.");
    }

    DataType keyType = col.type();
    DataType valueType = DataType.INTEGER;

    if (idxSchema.indexType() == IndexType.Hash) {
      return new HashIndex<>(
          tableSchema.tableName(),
          col.name(),
          idxSchema.indexFile(),
          keyType,
          valueType, // Tipe V (Value) -> Asumsi kita simpan Integer (RID)
          this.blockManager,
          this.serializer);
    } else if (idxSchema.indexType() == IndexType.BPlusTree) {
      int order = 100;
      return new BPlusIndex<>(
          tableSchema.tableName(),
          col.name(),
          order,
          idxSchema.indexFile(),
          this.blockManager,
          this.serializer);
    } else {
      throw new UnsupportedOperationException("Tipe indeks tidak dikenal: " + idxSchema.indexType());
    }
  }

  private void removeRowFromIndexes(Schema schema, long blockNumber, int slotId, Row row) {
    int ridValue = (int) ((blockNumber << 16) | (slotId & 0xFFFF));
    for (IndexSchema idxSchema : schema.indexes()) {
      @SuppressWarnings("unchecked")
      IIndex<Object, Integer> index = (IIndex<Object, Integer>) indexManager.get(
          schema.tableName(),
          idxSchema.columnName(),
          idxSchema.indexType().toString());
      if (index != null) {
        Object key = row.data().get(idxSchema.columnName());
        index.deleteData(key, ridValue);
        index.writeToFile(this.catalogManager);
      }
    }
  }

  private void populateIndexFromTable(Schema schema, IIndex<Object, Integer> index, String column)
      throws IOException {
    long blockCount = blockManager.getBlockCount(schema.dataFile());
    for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
      byte[] blockData = blockManager.readBlock(schema.dataFile(), blockNumber);
      int slotCount = serializer.getSlotCount(blockData);
      for (int slotId = 0; slotId < slotCount; slotId++) {
        Row row = serializer.readRowAtSlot(blockData, schema, slotId);
        if (row == null)
          continue;
        Object key = row.data().get(column);
        int ridValue = (int) ((blockNumber << 16) | (slotId & 0xFFFF));
        index.insertData(key, ridValue);
      }
    }
  }

  private List<ParsedFilter> parseFilterCondition(Object filterCondition, Schema schema) {
    if (filterCondition == null || schema == null) {
      return Collections.emptyList();
    }

    if (!(filterCondition instanceof String)) {
      return Collections.emptyList();
    }

    String raw = filterCondition.toString().trim();
    if (raw.isEmpty()) {
      return Collections.emptyList();
    }

    List<ParsedFilter> results = new ArrayList<>();
    String[] expressions = raw.split("(?i)\\s+AND\\s+");

    String[] operators = { ">=", "<=", "!=", "<>", "=", ">", "<" };

    for (String expression : expressions) {
      expression = expression.trim();
      String matchedOp = null;

      for (String op : operators) {
        if (expression.contains(op)) {
          matchedOp = op;
          break;
        }
      }

      if (matchedOp == null)
        continue;

      int opIndex = expression.indexOf(matchedOp);
      String columnName = expression.substring(0, opIndex).trim();
      String literalRaw = expression.substring(opIndex + matchedOp.length()).trim();

      Column column = schema.getColumnByName(columnName);
      if (column == null)
        continue;

      String literal = stripQuotes(literalRaw);
      Object typedValue = convertLiteral(column, literal);

      if (typedValue != null) {
        results.add(new ParsedFilter(column.name(), matchedOp, typedValue));
      }
    }
    return results;
  }

  private Object convertLiteral(Column column, String literal) {
    try {
      return switch (column.type()) {
        case INTEGER -> Integer.parseInt(literal);
        case FLOAT -> Float.parseFloat(literal);
        case CHAR -> literal.charAt(0);
        case VARCHAR -> literal;
      };
    } catch (Exception e) {
      return null;
    }
  }

  private String stripQuotes(String literal) {
    if (literal.length() >= 2) {
      if ((literal.startsWith("\"") && literal.endsWith("\"")) ||
          (literal.startsWith("'") && literal.endsWith("'"))) {
        return literal.substring(1, literal.length() - 1);
      }
    }
    return literal;
  }

  private boolean rowMatchesFilters(Row row, List<ParsedFilter> filters) {
    if (filters == null || filters.isEmpty()) {
      return true;
    }
    for (ParsedFilter filter : filters) {
      Object rowValue = row.data().get(filter.columnName);

      if (!compareValues(rowValue, filter.value, filter.operator)) {
        return false;
      }
    }
    return true;
  }

  private IndexType findIndexType(String indexType) {
    return Arrays.stream(IndexType.values())
        .filter(t -> t.name().equalsIgnoreCase(indexType))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Tipe indeks tidak dikenal: " + indexType));
  }

  private Row projectColumns(Row fullRow, List<String> requestedColumns) {
    if (requestedColumns == null || requestedColumns.contains("*")) {
      return fullRow; // Kembalikan semua jika minta "*"
    }

    Map<String, Object> projectedData = new HashMap<>();
    for (String colName : requestedColumns) {
      if (fullRow.data().containsKey(colName)) {
        projectedData.put(colName, fullRow.data().get(colName));
      }
    }
    return new Row(projectedData);
  }

  public BlockManager getBlockManager() {
    return this.blockManager;
  }

  // ==========================================
  // MAIN DRIVER UNTUK TEST JOIN
  // ==========================================
  public static void main(String[] args) {
    System.out.println("=== 1. Inisialisasi StorageManager ===");
    String dataDir = "../data";
    StorageManager sm = new StorageManager(dataDir);

    try {
      sm.initialize();
      System.out.println("Storage Manager berhasil diinisialisasi.");
    } catch (Exception e) {
      System.err.println("Gagal inisialisasi: " + e.getMessage());
      return;
    }

    // --- 2. Buat Tabel Induk: PRODI ---
    System.out.println("\n=== 2. Membuat Tabel 'prodi' (Induk) ===");
    try {
      List<Column> prodiCols = List.of(
          new Column("id_prodi", DataType.VARCHAR, 5),
          new Column("nama_prodi", DataType.VARCHAR, 100));
      List<IndexSchema> prodiIdx = List.of(
          new IndexSchema("idx_prodi_id", "id_prodi", IndexType.Hash, "prodi_id.idx"));
      // Tidak ada FK di tabel induk
      Schema prodiSchema = new Schema("prodi", "prodi.dat", prodiCols, prodiIdx, new ArrayList<>());

      sm.createTable(prodiSchema);
    } catch (IOException e) {
      System.err.println("Gagal buat prodi: " + e.getMessage());
    }

    // --- 3. Buat Tabel Anak: MAHASISWA (dengan FK) ---
    System.out.println("\n=== 3. Membuat Tabel 'mahasiswa' (Anak) dengan FK ===");
    try {
      List<Column> mhsCols = List.of(
          new Column("nim", DataType.VARCHAR, 10),
          new Column("nama", DataType.VARCHAR, 100),
          new Column("id_prodi", DataType.VARCHAR, 5) // Kolom FK
      );

      List<IndexSchema> mhsIdx = List.of(
          new IndexSchema("idx_mhs_nim", "nim", IndexType.Hash, "mhs_nim.idx"));

      // Definisi FK: mahasiswa.id_prodi -> prodi.id_prodi
      List<ForeignKeySchema> mhsFk = List.of(
          new ForeignKeySchema(
              "fk_mhs_prodi", // Nama constraint
              "id_prodi", // Kolom di mahasiswa
              "prodi", // Tabel referensi
              "id_prodi", // Kolom referensi
              true // On Delete Cascade
          ));

      Schema mhsSchema = new Schema("mahasiswa", "mahasiswa.dat", mhsCols, mhsIdx, mhsFk);
      sm.createTable(mhsSchema);

    } catch (IOException e) {
      System.err.println("Gagal buat mahasiswa: " + e.getMessage());
    }

    // --- 4. Insert Data (Persiapan Join) ---
    System.out.println("\n=== 4. Insert Data Dummy untuk Join ===");
    try {
      // Insert Prodi
      sm.writeBlock(new DataWrite("prodi", new Row(Map.of("id_prodi", "IF", "nama_prodi", "Informatika")), null));
      sm.writeBlock(new DataWrite("prodi", new Row(Map.of("id_prodi", "EL", "nama_prodi", "Elektro")), null));
      sm.writeBlock(new DataWrite("prodi", new Row(Map.of("id_prodi", "TI", "nama_prodi", "Teknik Industri")), null));

      // Insert Mahasiswa (dengan id_prodi yang valid)
      sm.writeBlock(
          new DataWrite("mahasiswa", new Row(Map.of("nim", "13523001", "nama", "Udin", "id_prodi", "IF")), null));
      sm.writeBlock(
          new DataWrite("mahasiswa", new Row(Map.of("nim", "13523002", "nama", "Asep", "id_prodi", "IF")), null));
      sm.writeBlock(
          new DataWrite("mahasiswa", new Row(Map.of("nim", "18223099", "nama", "Siti", "id_prodi", "EL")), null));

      System.out.println("Data berhasil dimasukkan.");
    } catch (Exception e) {
      System.err.println("Gagal insert: " + e.getMessage());
    }

    // --- 5. Verifikasi Read ---
    System.out.println("\n=== 5. Verifikasi Data Tersimpan ===");

    System.out.println("[Tabel Prodi]");
    List<Row> prodiData = sm.readBlock(new DataRetrieval("prodi", List.of("*"), null, false));
    prodiData.forEach(r -> System.out.println(" -> " + r.data()));

    System.out.println("\n[Tabel Mahasiswa]");
    List<Row> mhsData = sm.readBlock(new DataRetrieval("mahasiswa", List.of("*"), null, false));
    mhsData.forEach(r -> System.out.println(" -> " + r.data()));

    System.out.println("\nStorage siap untuk testing JOIN oleh Query Processor!");
    sm.shutdown();
  }

  @Override
  public int updateBlock(DataUpdate dataUpdate) {
    try {
      Schema schema = catalogManager.getSchema(dataUpdate.tableName());
      if (schema == null) {
        throw new IOException("Table not found: " + dataUpdate.tableName());
      }

      String fileName = schema.dataFile();
      Object filterRoot = dataUpdate.filterCondition();

      // 1. Parsing Filter
      List<ParsedFilter> parsedFilters = parseFilterCondition(filterRoot, schema);
      boolean isStringFilter = !(filterRoot instanceof WhereConditionNode) && filterRoot != null;

      int updatedRows = 0;
      List<Integer> ridsToUpdate = Collections.emptyList();
      boolean useIndexScan = false;

      // --- 2. OPTIMASI INDEX SCAN (Equality & Range) ---
      if (!parsedFilters.isEmpty()) {
        for (IndexSchema idxSchema : schema.indexes()) {
          IIndex index = indexManager.get(
              schema.tableName(),
              idxSchema.columnName(),
              idxSchema.indexType().toString());

          if (index == null)
            continue;

          // A. Equality Scan (Hash & BTree)
          Object eqValue = null;
          for (ParsedFilter pf : parsedFilters) {
            if (pf.columnName.equals(idxSchema.columnName()) && "=".equals(pf.operator)) {
              eqValue = pf.value;
              break;
            }
          }

          if (eqValue != null) {
            ridsToUpdate = index.getAddress(eqValue);
            useIndexScan = true;
          }
          // B. Range Scan (BTree only)
          else if (idxSchema.indexType() == IndexType.BPlusTree) {
            Object minVal = null;
            boolean minInclusive = true;
            Object maxVal = null;
            boolean maxInclusive = true;

            for (ParsedFilter pf : parsedFilters) {
              if (!pf.columnName.equals(idxSchema.columnName()))
                continue;
              if (">=".equals(pf.operator)) {
                minVal = pf.value;
                minInclusive = true;
              } else if (">".equals(pf.operator)) {
                minVal = pf.value;
                minInclusive = false;
              } else if ("<=".equals(pf.operator)) {
                maxVal = pf.value;
                maxInclusive = true;
              } else if ("<".equals(pf.operator)) {
                maxVal = pf.value;
                maxInclusive = false;
              }
            }

            if (minVal != null || maxVal != null) {
              if (index instanceof BPlusIndex) {
                ridsToUpdate = ((BPlusIndex) index).getAddresses((Comparable) minVal, minInclusive, (Comparable) maxVal,
                    maxInclusive);
                useIndexScan = true;
              }
            }
          }

          if (useIndexScan)
            break; // Cukup pakai satu index
        }
      }

      // --- 3. EXECUTION ---
      if (useIndexScan) {
        // A. STRATEGI INDEX SCAN
        for (int encodedRid : ridsToUpdate) {
          long blockNo = (encodedRid >>> 16);
          int slotNo = encodedRid & 0xFFFF;

          byte[] blockData = blockManager.readBlock(fileName, blockNo);
          Row row = serializer.readRowAtSlot(blockData, schema, slotNo);

          if (row != null) {
            boolean matches = isStringFilter ? rowMatchesFilters(row, parsedFilters)
                : evaluateCondition(row, filterRoot);

            if (matches) {
              // Proses Update (Sama seperti logika Full Scan, tapi per row)
              try {
                byte[] updatedBlock = serializer.updateRowInPlace(blockData, schema, slotNo, dataUpdate.updatedData());
                // Tulis blok segera karena loop ini melompat antar blok
                blockManager.writeBlock(fileName, blockNo, updatedBlock);
                updateIndexesForRow(schema, blockNo, slotNo, row, dataUpdate.updatedData());
                updatedRows++;
              } catch (IOException e) {
                // Fallback: Delete + Insert (Out-of-place update)
                Map<String, Object> mergedData = new HashMap<>(row.data());
                mergedData.putAll(dataUpdate.updatedData().data());
                Row newRow = new Row(mergedData);

                if (serializer.deleteSlot(blockData, slotNo)) {
                  blockManager.writeBlock(fileName, blockNo, blockData); // Commit delete
                  removeRowFromIndexes(schema, blockNo, slotNo, row);

                  // Insert new row (bisa di blok lain/akhir file)
                  DataWrite insertOp = new DataWrite(dataUpdate.tableName(), newRow, null);
                  writeBlock(insertOp);
                  updatedRows++;
                }
              }
            }
          }
        }
        blockManager.flush();
      } else {
        // B. STRATEGI FULL TABLE SCAN (Fallback)
        long blockCount = blockManager.getBlockCount(fileName);

        for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
          byte[] blockData = blockManager.readBlock(fileName, blockNumber);
          int slotCount = serializer.getSlotCount(blockData);
          boolean blockDirty = false;

          for (int slotId = 0; slotId < slotCount; slotId++) {
            Row row = serializer.readRowAtSlot(blockData, schema, slotId);
            if (row == null)
              continue;

            boolean matches = isStringFilter ? rowMatchesFilters(row, parsedFilters)
                : evaluateCondition(row, filterRoot);

            if (!matches)
              continue;

            try {
              // Coba In-Place Update
              byte[] updatedBlock = serializer.updateRowInPlace(blockData, schema, slotId, dataUpdate.updatedData());
              blockData = updatedBlock; // Update referensi memori blok
              blockDirty = true;
              updatedRows++;
              updateIndexesForRow(schema, blockNumber, slotId, row, dataUpdate.updatedData());
            } catch (IOException e) {
              // Fallback: Delete + Insert
              Map<String, Object> mergedData = new HashMap<>(row.data());
              mergedData.putAll(dataUpdate.updatedData().data());
              Row newRow = new Row(mergedData);

              if (serializer.deleteSlot(blockData, slotId)) {
                removeRowFromIndexes(schema, blockNumber, slotId, row);
                blockDirty = true;
              }

              // Jika blok kotor karena delete, tulis dulu sebelum insert (insert bisa baca
              // blok ini lagi)
              if (blockDirty) {
                blockManager.writeBlock(fileName, blockNumber, blockData);
                blockDirty = false;
              }

              DataWrite insertOp = new DataWrite(dataUpdate.tableName(), newRow, null);
              writeBlock(insertOp);
              updatedRows++;
            }
          }

          if (blockDirty) {
            blockManager.writeBlock(fileName, blockNumber, blockData);
          }
        }
        blockManager.flush();
      }

      return updatedRows;

    } catch (IOException e) {
      System.err.println("Error updating block: " + e.getMessage());
      return 0;
    }
  }

  /**
   * Helper: Update indeks jika kolom yang di-index berubah
   */
  private void updateIndexesForRow(Schema schema, long blockNumber, int slotId, Row oldRow, Row updatedData) {
    int ridValue = (int) ((blockNumber << 16) | (slotId & 0xffff));

    for (IndexSchema idxSchema : schema.indexes()) {
      @SuppressWarnings("unchecked")
      IIndex<Object, Integer> index = (IIndex<Object, Integer>) indexManager.get(schema.tableName(),
          idxSchema.columnName(), idxSchema.indexType().toString());

      if (index == null) {
        continue;
      }

      String colName = idxSchema.columnName();
      Object oldKey = oldRow.data().get(colName);
      Object newKey = updatedData.data().get(colName);

      // kalo kolom di index berubah, update index
      if (newKey != null && !newKey.equals(oldKey)) {
        // hapus old entry
        if (oldKey != null) {
          index.deleteData(oldKey, ridValue);
        }

        // insert new entry
        index.insertData(newKey, ridValue);
        index.writeToFile(this.catalogManager);
      }
    }
  }

  // helper for test di storage manager test
  public IndexManager getIndexManager() {
    return this.indexManager;
  }

  @Override
  public List<String> getDependentTables(String tableName) {
    return catalogManager.getDependentTables(tableName);
  }

  @Override
  public int dropTable(String tableName, String option) {
    try {
      Schema schema = catalogManager.getSchema(tableName);
      if (schema == null) {
        throw new IOException("Table not found: " + tableName);
      }

      List<String> dependents = getDependentTables(tableName);
      if (!dependents.isEmpty()) {
        if ("RESTRICT".equalsIgnoreCase(option)) {
          throw new RuntimeException(
              "DROP FAILED: Table '" + tableName + "' is referenced by " + dependents.toString());

        } else if ("CASCADE".equalsIgnoreCase(option)) {
          catalogManager.removeForeignKeyReferences(tableName);
        } else {
          throw new IllegalArgumentException("Invalid option: " + option + ". Use either 'CASCADE' or 'RESTRICT'");
        }

      }
      for (IndexSchema idx : schema.indexes()) {
        indexManager.drop(tableName, idx.columnName(), idx.indexType().toString());
      }
      blockManager.deleteFile(schema.dataFile());
      catalogManager.removeSchema(tableName);
      catalogManager.writeCatalog();
      return 0;
    } catch (IOException e) {
      System.err.println("Error during DROP TABLE: " + e.getMessage());
      return 1;
    }
  }
}
