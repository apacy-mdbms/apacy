package com.apacy.storagemanager;

import com.apacy.common.dto.*;
import com.apacy.common.enums.DataType;
import com.apacy.common.enums.IndexType;
import java.io.IOException;
import java.util.*;

public class DemoSeeder {
    
    private static final String DATA_DIR = "../data"; 

    public static void main(String[] args) {
        System.out.println("=== MULAI SEEDING DATA DEMO ===");
        
        // 1. Inisialisasi Storage Manager
        StorageManager sm = new StorageManager(DATA_DIR);
        try {
            sm.initialize();
        } catch (Exception e) {
            System.err.println("Gagal init SM: " + e.getMessage());
            return;
        }

        try {
            // --- 2. Create Table: Student ---
            System.out.println("Creating table 'Student'...");
            List<Column> studentCols = List.of(
                new Column("StudentID", DataType.INTEGER), // PK
                new Column("FullName", DataType.VARCHAR, 50),
                new Column("GPA", DataType.FLOAT)
            );
            // Asumsi: Primary Key biasanya butuh Index
            List<IndexSchema> studentIdx = List.of(
                new IndexSchema("idx_student_id", "StudentID", IndexType.Hash, "student_id.idx")
            );
            Schema studentSchema = new Schema("Student", "Student.dat", studentCols, studentIdx, new ArrayList<>());
            sm.createTable(studentSchema);

            // --- 3. Create Table: Course ---
            System.out.println("Creating table 'Course'...");
            List<Column> courseCols = List.of(
                new Column("CourseID", DataType.INTEGER), // PK
                new Column("Year", DataType.INTEGER),
                new Column("CourseName", DataType.VARCHAR, 50),
                new Column("CourseDescription", DataType.VARCHAR, 255) // LONGTEXT di map ke VARCHAR dulu
            );
            List<IndexSchema> courseIdx = List.of(
                new IndexSchema("idx_course_id", "CourseID", IndexType.Hash, "course_id.idx")
            );
            Schema courseSchema = new Schema("Course", "Course.dat", courseCols, courseIdx, new ArrayList<>());
            sm.createTable(courseSchema);

            // --- 4. Create Table: Attends (Junction) ---
            System.out.println("Creating table 'Attends'...");
            List<Column> attendsCols = List.of(
                new Column("StudentID", DataType.INTEGER),
                new Column("CourseID", DataType.INTEGER)
            );
            // Foreign Keys
            List<ForeignKeySchema> attendsFK = List.of(
                new ForeignKeySchema("fk_attends_student", "StudentID", "Student", "StudentID", true),
                new ForeignKeySchema("fk_attends_course", "CourseID", "Course", "CourseID", true)
            );
            // Index untuk mempercepat join
            List<IndexSchema> attendsIdx = List.of(
                new IndexSchema("idx_attends_student", "StudentID", IndexType.Hash, "attends_student.idx"),
                new IndexSchema("idx_attends_course", "CourseID", IndexType.Hash, "attends_course.idx")
            );
            Schema attendsSchema = new Schema("Attends", "Attends.dat", attendsCols, attendsIdx, attendsFK);
            sm.createTable(attendsSchema);

            // --- 5. Seeding Data (50 Baris per tabel) ---
            System.out.println("Inserting 50 rows per table...");
            
            Random rand = new Random();
            
            // Seed Student
            for (int i = 1; i <= 50; i++) {
                Map<String, Object> data = new HashMap<>();
                data.put("StudentID", i);
                data.put("FullName", "Student Name " + i);
                data.put("GPA", 2.0 + (rand.nextDouble() * 2.0)); // GPA 2.0 - 4.0
                
                sm.writeBlock(new DataWrite("Student", new Row(data), null));
            }

            // Seed Course
            for (int i = 101; i <= 150; i++) {
                Map<String, Object> data = new HashMap<>();
                data.put("CourseID", i);
                data.put("Year", 2020 + rand.nextInt(5)); // 2020-2024
                data.put("CourseName", "Course Subject " + i);
                data.put("CourseDescription", "Description for course " + i);
                
                sm.writeBlock(new DataWrite("Course", new Row(data), null));
            }

            // Seed Attends (Random relationship)
            for (int i = 0; i < 50; i++) {
                Map<String, Object> data = new HashMap<>();
                // Random student 1-50 ambil Random course 101-150
                data.put("StudentID", 1 + rand.nextInt(50));
                data.put("CourseID", 101 + rand.nextInt(50));
                
                sm.writeBlock(new DataWrite("Attends", new Row(data), null));
            }

            System.out.println("Seeding Selesai!");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            sm.shutdown(); // Penting untuk flush index dan catalog ke disk
        }
    }
}