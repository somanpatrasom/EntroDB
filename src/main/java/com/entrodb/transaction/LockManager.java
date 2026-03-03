package com.entrodb.transaction;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Row-level lock manager using 2-Phase Locking (2PL).
 *
 * Lock granularity: "tableId:pageNum:slot"
 * Table-level locks: "tableId"
 *
 * Protocol:
 *   Growing phase  — acquire locks as needed
 *   Shrinking phase — release all locks at commit/abort
 *
 * Deadlock prevention: timeout-based (wait max 5 seconds, then abort)
 */
public class LockManager {

    private static final long LOCK_TIMEOUT_MS = 5000;

    // lockKey → ReentrantReadWriteLock
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> locks
        = new ConcurrentHashMap<>();

    // txnId → set of lock keys held
    private final ConcurrentHashMap<Long, Set<String>> txnLocks
        = new ConcurrentHashMap<>();

    // ── Acquire ───────────────────────────────────────────────────

    /**
     * Acquire a shared (read) lock on a row.
     * Blocks if an exclusive lock is held by another transaction.
     */
    public void acquireShared(long txnId, String lockKey)
            throws InterruptedException {
        ReentrantReadWriteLock lock = getLock(lockKey);
        boolean acquired = lock.readLock()
            .tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (!acquired)
            throw new RuntimeException(
                "Lock timeout: transaction " + txnId
                + " waiting for SHARED lock on " + lockKey);
        recordLock(txnId, lockKey);
    }

    /**
     * Acquire an exclusive (write) lock on a row.
     * Blocks if any lock (shared or exclusive) is held by another transaction.
     */
    public void acquireExclusive(long txnId, String lockKey)
            throws InterruptedException {
        ReentrantReadWriteLock lock = getLock(lockKey);
        boolean acquired = lock.writeLock()
            .tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (!acquired)
            throw new RuntimeException(
                "Lock timeout: transaction " + txnId
                + " waiting for EXCLUSIVE lock on " + lockKey);
        recordLock(txnId, lockKey);
    }

    /** Acquire shared lock on entire table (for SELECT). */
    public void acquireTableShared(long txnId, String tableId)
            throws InterruptedException {
        acquireShared(txnId, "table:" + tableId);
    }

    /** Acquire exclusive lock on entire table (for DDL). */
    public void acquireTableExclusive(long txnId, String tableId)
            throws InterruptedException {
        acquireExclusive(txnId, "table:" + tableId);
    }

    /** Acquire exclusive lock on a specific row. */
    public void acquireRowExclusive(long txnId, String tableId,
                                     int pageNum, int slot)
            throws InterruptedException {
        acquireExclusive(txnId, rowKey(tableId, pageNum, slot));
    }

    /** Acquire shared lock on a specific row. */
    public void acquireRowShared(long txnId, String tableId,
                                  int pageNum, int slot)
            throws InterruptedException {
        acquireShared(txnId, rowKey(tableId, pageNum, slot));
    }

    // ── Release ───────────────────────────────────────────────────

    /**
     * Release ALL locks held by a transaction.
     * Called at commit or abort (shrinking phase of 2PL).
     */
    public void releaseAll(long txnId) {
        Set<String> held = txnLocks.remove(txnId);
        if (held == null) return;

        for (String key : held) {
            ReentrantReadWriteLock lock = locks.get(key);
            if (lock == null) continue;

            // Try to release write lock first, then read lock
            if (lock.isWriteLockedByCurrentThread()) {
                lock.writeLock().unlock();
            } else if (lock.getReadHoldCount() > 0) {
                lock.readLock().unlock();
            }

            // Remove lock entry if no one is waiting
            if (!lock.hasQueuedThreads()
                    && lock.getReadLockCount() == 0
                    && !lock.isWriteLocked()) {
                locks.remove(key);
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────

    private ReentrantReadWriteLock getLock(String key) {
        return locks.computeIfAbsent(key,
            k -> new ReentrantReadWriteLock(true)); // fair = true (FIFO)
    }

    private void recordLock(long txnId, String key) {
        txnLocks.computeIfAbsent(txnId,
            k -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(key);
    }

    private String rowKey(String tableId, int pageNum, int slot) {
        return tableId + ":" + pageNum + ":" + slot;
    }

    public int getActiveLockCount() {
        return locks.size();
    }

    public int getTransactionLockCount(long txnId) {
        Set<String> held = txnLocks.get(txnId);
        return held == null ? 0 : held.size();
    }
}
