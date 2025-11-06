# mDBMS-Apacy

**Modular Database Management System - Super Group Apacy**

## Overview

mDBMS-Apacy adalah sistem manajemen basis data modular komprehensif yang dibangun dari awal menggunakan Java 17 dan Maven. Proyek ini merupakan tugas akademis yang mendemonstrasikan implementasi komponen inti DBMS termasuk *storage management*, *query processing*, *concurrency control*, dan *failure recovery*.

## Architecture

Sistem ini diorganisir sebagai *monorepo* yang terdiri dari 6 modul Maven utama:

1.  **Common (`common/`)**: Modul "Kitab Suci" yang berisi DTOs (`record`), `enum`, dan `interface` yang digunakan bersama oleh semua modul lain.
2.  **Storage Manager (`storage-manager/`)**: Menangani penyimpanan data fisik, manajemen blok, serialisasi, *indexing* (Hash, B+ Tree), dan pengumpulan statistik.
3.  **Query Optimizer (`query-optimizer/`)**: Bertanggung jawab untuk mem-parsing string SQL menjadi AST (`ParsedQuery`) dan menerapkan aturan optimasi berbasis heuristik.
4.  **Concurrency Control (`concurrency-control/`)**: Mengelola akses data secara konkuren menggunakan *locking* (`LockManager`) dan manajemen transaksi.
5.  **Failure Recovery (`failure-recovery/`)**: Menjamin konsistensi dan pemulihan data melalui *logging* (`LogWriter`) dan mekanisme *recovery* (`LogReplayer`).
6.  **Query Processor (`query-processor/`)**: Mesin eksekusi utama dan titik masuk CLI (`Main.java`). Modul ini mengoordinasikan semua modul lain untuk mengeksekusi *query* dari awal hingga akhir.

## Module Dependencies

Proyek ini mengikuti graf dependensi yang jelas, yang diatur oleh Maven:
* **Query Processor (`query-processor/`)** adalah integrator pusat dan bergantung pada 5 modul lainnya.
* Semua 4 komponen lainnya (SM, QO, CCM, FRM) **hanya bergantung pada `common`**.
* `common` tidak memiliki dependensi.



## Getting Started

### Prerequisites
* Java 17 (Temurin atau yang setara)
* Maven 3.6+

### Build
*Compile* dan *build* semua 6 modul dari direktori *root* proyek:
```bash
mvn clean install
````

### Test

Jalankan *semua* unit test untuk *semua* 6 modul:

```bash
mvn test
```

### Run

Titik masuk utama adalah CLI di modul `query-processor`.

```bash
# Jalankan dari direktori root proyek
mvn -pl query-processor exec:java -Dexec.mainClass="com.apacy.queryprocessor.Main"
```

## Development Workflow

Ini adalah *monorepo* `public` yang digunakan bersama oleh 5 tim (25+ developer). Untuk mencegah kekacauan, kita memberlakukan alur kerja yang ketat menggunakan GitHub Actions (CI) dan `CODEOWNERS`.

**Semua pekerjaan WAJIB mengikuti panduan di [dev_workflow.md](dev_workflow.md).** (Harap baca file tersebut sebelum memulai).

**Aturan Kunci:**

1.  **Branch:** Semua *branch* harus mengikuti format `feat/<komponen>/<fitur>` (misal: `feat/query-processor/implement-join`).
2.  **PR:** Semua kode harus masuk ke `main` melalui **Pull Request (PR)**.
3.  **CI:** Semua PR harus lolos *CI checks* (`mvn clean install` & `mvn test`) sebelum bisa di-*merge*.
4.  **Review:** Semua PR wajib mendapatkan minimal satu *approval* dari `CODEOWNERS` komponen tersebut (misal: `@apacy-mdbms/team-qp` untuk perubahan di `query-processor/`).

## License

Proyek ini adalah bagian dari tugas akademis untuk mata kuliah IF3140 Sistem Basis Data.