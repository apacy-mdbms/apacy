package com.apacy.storagemanager;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.dto.Statistic;
import com.apacy.common.enums.IndexType;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Statistics Collector gathers and maintains database statistics for query optimization.
 * Mengimplementasikan logika untuk get_stats()
 */
public class StatsCollector {
    
    // StatsCollector butuh akses ke semua komponen internal SM
    private final CatalogManager catalogManager;
    private final BlockManager blockManager;
    private final Serializer serializer;

    public StatsCollector(CatalogManager catalogManager, BlockManager blockManager, Serializer serializer) {
        this.catalogManager = catalogManager;
        this.blockManager = blockManager;
        this.serializer = serializer;
    }
    
    /**
     * Dipanggil oleh StorageManager.getAllStats().
     * Mengumpulkan statistik untuk SEMUA tabel yang ada di katalog.
     */
    public Map<String, Statistic> getAllStats() {
        Map<String, Statistic> allStats = new HashMap<>();
        // Ambil semua skema dari catalog manager
        Collection<Schema> allSchemas = catalogManager.getAllSchemas();

        // Iterasi setiap tabel dan kumpulkan statistiknya
        for (Schema schema : allSchemas) {
            try {
                Statistic stats = collectStatsForTable(schema);
                allStats.put(schema.tableName(), stats);
            } catch (IOException e) {
                System.err.println("StatsCollector: Gagal mengumpulkan statistik untuk tabel " + schema.tableName() + ": " + e.getMessage());
                // Masukkan statistik kosong jika gagal
                allStats.put(schema.tableName(), new Statistic(0, 0, 0, 0, Map.of(), Map.of()));
            }
        }
        return allStats;
    }

    /**
     * Logika utama: Melakukan Full Table Scan untuk 1 tabel dan menghitung metriknya.
     */
    private Statistic collectStatsForTable(Schema schema) throws IOException {
        String tableName = schema.tableName();
        String dataFile = schema.dataFile();

        int nr = 0; // jumlah tuple
        long br = 0; // jumlah blok
        long totalRowSize = 0; // total ukuran byte semua tuple (untuk menghitung lr)
        
        // Map untuk V(A,r): Map<NamaKolom, Set<NilaiUnik>>
        Map<String, Set<Object>> distinctValues = new HashMap<>();
        // Inisialisasi Set untuk setiap kolom di tabel ini
        for (Column col : schema.columns()) {
            distinctValues.put(col.name(), new HashSet<>());
        }

        // 1. Dapatkan jumlah blok (br)
        br = blockManager.getBlockCount(dataFile);

        // 2. Iterasi setiap blok untuk menghitung nr, lr, dan V(A,r)
        for (long blockNumber = 0; blockNumber < br; blockNumber++) {
            byte[] blockData = blockManager.readBlock(dataFile, blockNumber);
            
            // 3. Deserialize blok (mendapatkan List<Row>)
            // Kita perlu serializer untuk membaca struktur Slotted Page
            List<Row> rowsInBlock = serializer.deserializeBlock(blockData, schema);

            if (rowsInBlock != null && !rowsInBlock.isEmpty()) {
                for (Row row : rowsInBlock) {
                    // 4. Hitung nr (jumlah row)
                    nr++;
                    
                    // 5. Akumulasi total ukuran (menggunakan estimator serializer)
                    totalRowSize += serializer.estimateSize(row, schema);

                    // 6. Kumpulkan nilai unik untuk V(A,r)
                    for (Map.Entry<String, Object> entry : row.data().entrySet()) {
                        String columnName = entry.getKey();
                        Object value = entry.getValue();
                        
                        // Tambahkan nilai ke Set (duplikat akan diabaikan oleh HashSet)
                        if (distinctValues.containsKey(columnName)) {
                            distinctValues.get(columnName).add(value);
                        }
                    }
                }
            }
        }

        // 7. Hitung statistik final (lr, fr)
        int lr = (nr == 0) ? 0 : (int) (totalRowSize / nr); 
        
        int blockSize = blockManager.getBlockSize();
        
        // fr: blocking factor
        int fr = (lr == 0) ? 0 : (blockSize / lr); 

        // 8. Konversi Map<String, Set<Object>> ke Map<String, Integer> untuk V(A,r)
        Map<String, Integer> V = new HashMap<>();
        for (Map.Entry<String, Set<Object>> entry : distinctValues.entrySet()) {
            V.put(entry.getKey(), entry.getValue().size());
        }

        // 9. Dapatkan info indeks dari skema
        Map<String, IndexType> indexedColumn = schema.indexes().stream()
            .collect(Collectors.toMap(
                IndexSchema::columnName, 
                IndexSchema::indexType,
                (existingType, newType) -> existingType // Jaga-jaga jika ada duplikat
            ));

        // 10. Kembalikan objek Statistic
        return new Statistic(nr, (int) br, lr, fr, V, indexedColumn);
    }
}