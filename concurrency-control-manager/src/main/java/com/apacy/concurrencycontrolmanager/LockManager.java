package com.apacy.concurrencycontrolmanager;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Lock Manager dengan Two-Phase Locking (2PL)
 * pencegahan deadlock NON-BLOCKING (pakai Response=false) menggunakan strategi Wound-Wait.
 *
 * Metode acquire mengembalikan boolean (true jika lock didapat, false jika harus WAIT atau di-WOUND).
 */
public class LockManager {

    private enum LockType {
        SHARED,
        EXCLUSIVE
    }

    private class Lock {
        Transaction transaction;
        LockType type;

        Lock(Transaction transaction, LockType type) {
            this.transaction = transaction;
            this.type = type;
        }
    }

    /**
     * Entri dalam Lock Table. Setiap resource (data item) memiliki satu entri.
     */
    private class LockTableEntry {
        List<Lock> holders = new ArrayList<>(); // Daftar transaksi yang saat ini memegang lock
        LockType currentLockType = null;        // Tipe lock yang sedang dipegang

        boolean hasExclusiveLock() {
            return currentLockType == LockType.EXCLUSIVE;
        }
        boolean isLocked() {
            return currentLockType != null;
        }
        Transaction getExclusiveHolder() {
            if (hasExclusiveLock() && !holders.isEmpty()) {
                return holders.get(0).transaction;
            }
            return null;
        }
        List<Transaction> getAllHolders() {
            List<Transaction> txs = new ArrayList<>();
            for (Lock lock : holders) {
                txs.add(lock.transaction);
            }
            return txs;
        }
    }

    /**
     * Lock Table utama
     * memetakan resourceId (String) ke LockTableEntry-nya
     */
    private final Map<String, LockTableEntry> lockTable = new HashMap<>();

    public LockManager() {
        // Sudah di-inisialisasi
    }

    /**
     * Mendapatkan atau membuat LockTableEntry untuk resourceId.
     * di-sinkronisasi untuk thread-safety saat mengakses/membuat entri baru di lockTable.
     */
    private synchronized LockTableEntry getOrCreateEntry(String resourceId) {
        return lockTable.computeIfAbsent(resourceId, k -> new LockTableEntry());
    }

    /**
     * Meminta Shared (read) lock pada sebuah resource (Non-Blocking).
     *
     * @param resourceId Resource yang ingin di-lock
     * @param transaction Transaksi yang meminta lock
     * @return true jika lock didapat, false jika harus menunggu.
     */
    public boolean acquireSharedLock(String resourceId, Transaction transaction) {
        LockTableEntry entry = getOrCreateEntry(resourceId);

        // Sinkronisasi pada level 'entry'
        // mengizinkan konkurensi pada resource yang berbeda.
        synchronized (entry) {
            // 1. Cek apakah transaksi ini di-ABORT
            if (transaction.isAborted()) {
                return false; // Transaksi sudah di-abort
            }

            // 2. Cek apakah lock bisa diberikan (tidak ada X-lock)
            if (!entry.hasExclusiveLock()) {
                // Cek apakah tx ini sudah memegang S-lock
                for(Lock l : entry.holders) {
                    if (l.transaction == transaction) {
                        return true; // Sudah memegang lock
                    }
                }
                // Berikan S-lock baru
                entry.holders.add(new Lock(transaction, LockType.SHARED));
                entry.currentLockType = LockType.SHARED;
                return true; // Lock didapat!
            }

            // 3. Ada konflik (ada lock EKSKLUSIF oleh T_holder)
            Transaction holder = entry.getExclusiveHolder();
            if (holder == transaction) {
                // Agak aneh, tapi tx ini udah megang X-lock, jadi S-lock diperbolehkan
                return true;
            }
            
            // 4. Logika Wound-Wait
            if (transaction.getTimestamp() < holder.getTimestamp()) {
                // Requester (transaction) LEBIH TUA dari holder.
                // WOUND: Abort si holder.
                holder.setStatus(Transaction.TransactionStatus.ABORTED);
            }
            // else: Requester LEBIH MUDA dari holder.
            // WAIT: Requester harus menunggu.

            // Dalam kedua kasus (Wound atau Wait), requester GAGAL mendapat lock SAAT INI.
            return false;
        }
    }

    /**
     * Meminta Exclusive (write) lock pada sebuah resource (Non-Blocking).
     *
     * @param resourceId Resource yang ingin di-lock
     * @param transaction Transaksi yang meminta lock
     * @return true jika lock didapat/di-upgrade, false jika harus menunggu.
     */
    public boolean acquireExclusiveLock(String resourceId, Transaction transaction) {
        LockTableEntry entry = getOrCreateEntry(resourceId);

        synchronized (entry) {
            // 1. Cek apakah transaksi ini di-abort
            if (transaction.isAborted()) {
                return false;
            }

            // 2. Cek apakah lock bisa diberikan (tidak ada lock sama sekali)
            if (!entry.isLocked()) {
                entry.holders.add(new Lock(transaction, LockType.EXCLUSIVE));
                entry.currentLockType = LockType.EXCLUSIVE;
                return true; // Lock didapat!
            }

            // 3. Cek apakah dipegang oleh transaksi ini sendiri
            if (entry.holders.size() == 1 && entry.holders.get(0).transaction == transaction) {
                // Jika sudah X-lock, return true
                if (entry.currentLockType == LockType.EXCLUSIVE) {
                    return true;
                }
                // Jika S-lock, lakukan Lock Upgrade
                if (entry.currentLockType == LockType.SHARED) {
                    entry.holders.clear();
                    entry.holders.add(new Lock(transaction, LockType.EXCLUSIVE));
                    entry.currentLockType = LockType.EXCLUSIVE;
                    return true; // Lock upgrade berhasil!
                }
            }

            // 4. Ada konflik (ada lock S atau X oleh transaksi lain)
            List<Transaction> allHolders = entry.getAllHolders();

            // 5. Logika Wound-Wait
            for (Transaction holder : allHolders) {
                if (holder != transaction) {
                    if (transaction.getTimestamp() < holder.getTimestamp()) {
                        // Requester (transaction) LEBIH TUA dari holder.
                        // WOUND: Abort si holder.
                        holder.setStatus(Transaction.TransactionStatus.ABORTED);
                    }
                    // else: Requester LEBIH MUDA dari holder.
                    // WAIT: Requester harus menunggu.
                }
            }

            // Dalam kedua kasus (Wound atau Wait), requester GAGAL mendapat lock SAAT INI.
            return false;
        }
    }

    /**
     * Melepaskan SEMUA lock yang dipegang oleh sebuah transaksi
     * dipanggil saat transaksi Selesai (COMMIT atau ABORT)
     *
     * @param transaction Transaksi yang melepaskan lock
     */
    public synchronized void releaseLocks(Transaction transaction) {
        // Kunci seluruh lockTable untuk mencegah sedang iterasi.
        
        for (LockTableEntry entry : lockTable.values()) {
            synchronized (entry) {
                // Hapus semua lock yang dipegang oleh transaksi ini
                boolean lockReleased = entry.holders.removeIf(
                    lock -> lock.transaction == transaction
                );

                if (lockReleased) {
                    // Jika lock dilepas DAN entry menjadi kosong
                    if (entry.holders.isEmpty()) {
                        entry.currentLockType = null;
                    }
                }
            }
        }
    }


    /**
     * Get current lock status for debugging/monitoring.
     */
    public synchronized String getLockStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("--- Lock Manager Status (Non-Blocking) ---\n");
        for (Map.Entry<String, LockTableEntry> mapEntry : lockTable.entrySet()) {
            String resourceId = mapEntry.getKey();
            LockTableEntry entry = mapEntry.getValue();
            
            synchronized (entry) { // Sinkronisasi saat membaca
                if (!entry.isLocked()) continue;

                sb.append("Resource: ").append(resourceId).append("\n");
                sb.append("  Mode: ").append(entry.currentLockType).append("\n");
                sb.append("  Holders:\n");
                for (Lock lock : entry.holders) {
                    sb.append("    - T_ID: ").append(lock.transaction.getTransactionId())
                      .append(" (TS: ").append(lock.transaction.getTimestamp()).append(")\n");
                }
            }
        }
        sb.append("-------------------------------------------\n");
        return sb.toString();
    }
}