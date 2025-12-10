package com.apacy.storagemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.apacy.common.dto.Column;
import com.apacy.common.dto.DataWrite;
import com.apacy.common.dto.ForeignKeySchema;
import com.apacy.common.dto.IndexSchema;
import com.apacy.common.dto.Row;
import com.apacy.common.dto.Schema;
import com.apacy.common.enums.DataType;
import com.apacy.common.enums.IndexType;

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
            // Drop existing tables if any
            System.out.println("Checking for existing tables...");
            try {
                sm.dropTable("Attends", "CASCADE");
                System.out.println("  -> Dropped existing 'Attends' table");
            } catch (Exception e) {
                // Table doesn't exist, that's fine
            }
            try {
                sm.dropTable("Student", "CASCADE");
                System.out.println("  -> Dropped existing 'Student' table");
            } catch (Exception e) {
                // Table doesn't exist, that's fine
            }
            try {
                sm.dropTable("Course", "CASCADE");
                System.out.println("  -> Dropped existing 'Course' table");
            } catch (Exception e) {
                // Table doesn't exist, that's fine
            }
            
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
            System.out.println("Inserting data...");
            
            Random rand = new Random(42); // Fixed seed untuk konsistensi
            
            // Nama depan dan belakang untuk generate nama mahasiswa
            String[] firstNames = {"Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Hannah", 
                                   "Ian", "Julia", "Kevin", "Laura", "Michael", "Nancy", "Oliver", "Patricia",
                                   "Quinn", "Rachel", "Steven", "Teresa", "Uma", "Victor", "Wendy", "Xavier",
                                   "Yolanda", "Zachary", "Amy", "Benjamin", "Catherine", "David"};
            String[] lastNames = {"Johnson", "Smith", "Brown", "Davis", "Wilson", "Moore", "Taylor", "Anderson",
                                  "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez",
                                  "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen",
                                  "Young", "King", "Wright", "Lopez", "Hill", "Scott"};
            
            // Seed Student (50 baris)
            System.out.println("  -> Inserting 50 students...");
            for (int i = 1; i <= 50; i++) {
                Map<String, Object> data = new HashMap<>();
                data.put("StudentID", i);
                String fullName = firstNames[rand.nextInt(firstNames.length)] + " " + 
                                 lastNames[rand.nextInt(lastNames.length)];
                data.put("FullName", fullName);
                // GPA antara 2.0 - 4.0
                data.put("GPA", Math.round((2.0 + (rand.nextDouble() * 2.0)) * 10.0) / 10.0);
                
                sm.writeBlock(new DataWrite("Student", new Row(data), null));
            }
            System.out.println("     ✓ 50 students inserted");

            // Seed Course (50 baris)
            System.out.println("  -> Inserting 50 courses...");
            String[] courseNames = {
                "Introduction to Databases", "Data Structures", "Operating Systems", 
                "Computer Networks", "Software Engineering", "Web Development",
                "Mobile Application Development", "Machine Learning", "Artificial Intelligence",
                "Computer Graphics", "Algorithms", "Discrete Mathematics",
                "Linear Algebra", "Calculus I", "Calculus II",
                "Physics I", "Physics II", "Chemistry",
                "Statistics", "Probability Theory", "Digital Logic Design",
                "Computer Architecture", "Compiler Design", "Programming Languages",
                "Database Management Systems", "Data Mining", "Big Data Analytics",
                "Cloud Computing", "Cybersecurity", "Cryptography",
                "Human-Computer Interaction", "Computer Vision", "Natural Language Processing",
                "Distributed Systems", "Parallel Computing", "Quantum Computing",
                "Blockchain Technology", "Internet of Things", "Embedded Systems",
                "Game Development", "Computer Ethics", "Technical Writing",
                "Project Management", "Entrepreneurship", "Business Analytics",
                "Information Systems", "IT Infrastructure", "Network Security",
                "System Administration", "DevOps"
            };
            
            for (int i = 1; i <= 50; i++) {
                Map<String, Object> data = new HashMap<>();
                data.put("CourseID", 100 + i); // 101-150
                data.put("Year", 2023 + (i % 2)); // 2023 atau 2024
                data.put("CourseName", courseNames[i - 1]);
                data.put("CourseDescription", "A comprehensive course covering " + 
                        courseNames[i - 1].toLowerCase() + " concepts and applications.");
                
                sm.writeBlock(new DataWrite("Course", new Row(data), null));
            }
            System.out.println("     ✓ 50 courses inserted");

            // Seed Attends (50 baris) - pastikan kombinasi StudentID & CourseID unik
            System.out.println("  -> Inserting 50 attendance records...");
            Set<String> attendsSet = new HashSet<>();
            int attendsCount = 0;
            int attempts = 0;
            int maxAttempts = 500; // Prevent infinite loop
            
            while (attendsCount < 50 && attempts < maxAttempts) {
                attempts++;
                int studentId = 1 + rand.nextInt(50); // 1-50
                int courseId = 101 + rand.nextInt(50); // 101-150
                String key = studentId + "-" + courseId;
                
                // Pastikan kombinasi unik (karena composite primary key)
                if (!attendsSet.contains(key)) {
                    attendsSet.add(key);
                    Map<String, Object> data = new HashMap<>();
                    data.put("StudentID", studentId);
                    data.put("CourseID", courseId);
                    
                    sm.writeBlock(new DataWrite("Attends", new Row(data), null));
                    attendsCount++;
                }
            }
            System.out.println("     ✓ " + attendsCount + " attendance records inserted");

            System.out.println("\n=== SEEDING COMPLETED SUCCESSFULLY ===");
            System.out.println("Summary:");
            System.out.println("  - Student table: 50 rows");
            System.out.println("  - Course table: 50 rows");
            System.out.println("  - Attends table: 50 rows");
            System.out.println("  Total: 150 rows inserted");
            System.out.println("======================================");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            sm.shutdown(); // Penting untuk flush index dan catalog ke disk
        }
    }
}