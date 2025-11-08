package com.apacy.storagemanager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

// class StorageManagerTest {
    
//     private StorageManager storageManager;
    
//     @BeforeEach
//     void setUp() {
//         storageManager = new StorageManager("Bruh");
//     }
    
//     @Test
//     void testComponentName() {
//         assertEquals("Storage Manager", storageManager.getComponentName());
//     }
    
//     @Test
//     void testInitialize() throws Exception {
//         assertDoesNotThrow(() -> storageManager.initialize());
//     }
    
//     @Test
//     void testShutdown() {
//         assertDoesNotThrow(() -> storageManager.shutdown());
//     }
// }

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class StorageManagerTest {
    private static BlockManager blockManager;
    private static Serializer serializer;
    
    // Path ke folder data
    private static final String DATA_DIR = "data"; 
    // Kita pakai file tes terpisah agar tidak merusak student.dat asli
    private static final String TEST_FILE = "student_physical_test.dat"; 

    @BeforeAll
    public static void setup() {
        blockManager = new BlockManager(DATA_DIR); 
        serializer = new Serializer();
        
        try {
            Files.deleteIfExists(Paths.get(DATA_DIR, TEST_FILE));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * TES INTEGRASi (PHYSICAL LAYER)
     * Tes alur: Row -> Serialize -> WriteBlock -> ReadBlock -> Deserialize -> Row
     */
    @Test
    public void testPhysicalLayerIntegration_WriteThenRead() {
        System.out.println("Menjalankan tes: testPhysicalLayerIntegration_WriteThenRead");
        
        try {
            // 1. Definisikan Skema Student
            Schema studentSchema = new Schema(Arrays.asList(
                new Column("StudentID", DataType.INTEGER),      // 4 bytes
                new Column("FullName", DataType.VARCHAR, 50),  // 50 bytes
                new Column("GPA", DataType.FLOAT)               // 4 bytes
            )); // Total Row Size: 58 bytes
            
            int rowSize = studentSchema.getFixedRowSize();

            // 2. Buat Data Row Asli (di Memori)
            Row rowAsli = new Row(studentSchema, Arrays.asList(13519001, "Budi Dharma", 3.85f));

            // 3. Panggil Serializer (Pekerjaan Anda)
            byte[] rowBytes = serializer.serialize(rowAsli);
            Assertions.assertEquals(rowSize, rowBytes.length, "Ukuran serialize tidak cocok dengan skema");

            // 4. Siapkan Blok
            byte[] blockData = new byte[blockManager.getBlockSize()];
            
            //  salin data Row ke dalam blok
            System.arraycopy(rowBytes, 0, blockData, 0, rowBytes.length);
            
            // 5. BlockManager.writeBlock 
            blockManager.writeBlock(TEST_FILE, 0, blockData);
            System.out.println("-> Berhasil menulis ke file: " + TEST_FILE);

            // 6. Panggil BlockManager.readBlock
            byte[] readBlockData = blockManager.readBlock(TEST_FILE, 0);
            System.out.println("-> Berhasil membaca dari file: " + TEST_FILE);

            // 7. Unpack Blok 
            byte[] readRowBytes = Arrays.copyOfRange(readBlockData, 0, rowSize);

            // 8. Serializer.deserialize
            Row rowPulih = serializer.deserialize(readRowBytes, studentSchema);

            // 9. VERIFIKASI
            System.out.println("Data Asli   : " + rowAsli.getValue(1));
            System.out.println("Data Pulih  : " + rowPulih.getValue(1));
            
            Assertions.assertEquals(rowAsli.getValue(0), rowPulih.getValue(0), "StudentID tidak cocok!");
            Assertions.assertEquals(rowAsli.getValue(1), rowPulih.getValue(1), "FullName tidak cocok!");
            Assertions.assertEquals((Float) rowAsli.getValue(2), (Float) rowPulih.getValue(2), 0.001f, "GPA tidak cocok!");
            
            System.out.println("-> [HASIL]: SUKSES! Tes integrasi Physical Layer (Orang 1) berhasil.");

        } catch (IOException e) {
            Assertions.fail("Tes gagal karena exception: " + e.getMessage());
        }
    }
}