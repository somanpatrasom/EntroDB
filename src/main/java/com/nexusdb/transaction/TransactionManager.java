package com.nexusdb.transaction;

import com.nexusdb.buffer.BufferPoolManager;
import com.nexusdb.catalog.CatalogManager;
import com.nexusdb.storage.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {

    private final WALManager             wal;
    private final BufferPoolManager      bpm;
    private final CatalogManager         catalog;
    private final AtomicLong             txnIdCounter;
    private final Map<Long, Transaction> activeTxns;

    public TransactionManager(WALManager wal, BufferPoolManager bpm,
                               CatalogManager catalog) {
        this.wal          = wal;
        this.bpm          = bpm;
        this.catalog      = catalog;
        this.txnIdCounter = new AtomicLong(1);
        this.activeTxns   = new ConcurrentHashMap<>();
    }

    // ── Transaction Lifecycle ─────────────────────────────────────

    public Transaction begin() throws IOException {
        long txnId = txnIdCounter.getAndIncrement();
        Transaction txn = new Transaction(txnId);

        WALRecord beginRecord = new WALRecord(wal.nextLSN(), txnId, WALRecord.Type.BEGIN);
        long lsn = wal.append(beginRecord);
        txn.addLSN(lsn);

        activeTxns.put(txnId, txn);
        return txn;
    }

    public void commit(Transaction txn) throws IOException {
        if (!txn.isActive())
            throw new IllegalStateException("Transaction not active: " + txn.getTxnId());

        WALRecord commitRecord = new WALRecord(wal.nextLSN(), txn.getTxnId(), WALRecord.Type.COMMIT);
        long lsn = wal.append(commitRecord);
        txn.addLSN(lsn);
        txn.commit();

        activeTxns.remove(txn.getTxnId());
        System.out.println("TXN COMMIT: " + txn.getSummary());
    }

    public void abort(Transaction txn) throws IOException {
        if (!txn.isActive()) return;

        System.out.println("TXN ABORT: rolling back " + txn.getSummary());

        List<Long> lsns = txn.getWALLSNs();
        List<WALRecord> allRecords = wal.readAll();

        for (int i = lsns.size() - 1; i >= 0; i--) {
            long lsn = lsns.get(i);
            WALRecord record = allRecords.stream()
                .filter(r -> r.lsn == lsn)
                .findFirst().orElse(null);
            if (record == null) continue;
            undoRecord(record);
        }

        WALRecord abortRecord = new WALRecord(
            wal.nextLSN(), txn.getTxnId(), WALRecord.Type.ABORT);
        wal.append(abortRecord);
        txn.abort();
        activeTxns.remove(txn.getTxnId());
    }

    // ── WAL Logging ───────────────────────────────────────────────

    public long logInsert(Transaction txn, String tableId, int pageNum,
                           int slot, byte[] record) throws IOException {
        WALRecord r = new WALRecord(
            wal.nextLSN(), txn.getTxnId(), WALRecord.Type.INSERT,
            tableId, pageNum, slot, null, record);
        long lsn = wal.append(r);
        txn.addLSN(lsn);
        return lsn;
    }

    public long logUpdate(Transaction txn, String tableId, int pageNum,
                           int slot, byte[] before, byte[] after) throws IOException {
        WALRecord r = new WALRecord(
            wal.nextLSN(), txn.getTxnId(), WALRecord.Type.UPDATE,
            tableId, pageNum, slot, before, after);
        long lsn = wal.append(r);
        txn.addLSN(lsn);
        return lsn;
    }

    public long logDelete(Transaction txn, String tableId, int pageNum,
                           int slot, byte[] before) throws IOException {
        WALRecord r = new WALRecord(
            wal.nextLSN(), txn.getTxnId(), WALRecord.Type.DELETE,
            tableId, pageNum, slot, before, null);
        long lsn = wal.append(r);
        txn.addLSN(lsn);
        return lsn;
    }

    // ── Recovery ──────────────────────────────────────────────────

    public void recover() throws IOException {
        List<WALRecord> records = wal.readAll();
        if (records.isEmpty()) {
            System.out.println("WAL: nothing to recover");
            return;
        }

        System.out.println("WAL: recovering from " + records.size() + " records...");

        // Find committed and aborted transaction IDs
        Set<Long> committed = new HashSet<>();
        Set<Long> aborted   = new HashSet<>();
        for (WALRecord r : records) {
            if (r.type == WALRecord.Type.COMMIT) committed.add(r.txnId);
            if (r.type == WALRecord.Type.ABORT)  aborted.add(r.txnId);
        }

        // REDO: replay committed transactions — skip if pageLSN >= record LSN
        int redone  = 0;
        int skipped = 0;
        for (WALRecord r : records) {
            if (!committed.contains(r.txnId)) continue;
            if (r.type != WALRecord.Type.INSERT &&
                r.type != WALRecord.Type.UPDATE &&
                r.type != WALRecord.Type.DELETE) continue;
            if (r.tableId == null) continue;

            // Check pageLSN — skip if this change is already on disk
            PageId pid   = new PageId(r.tableId, r.pageNum);
            Page   page  = bpm.fetchPage(pid);
            long pageLSN = page.getPageLSN();
            bpm.unpinPage(pid, false);

            if (pageLSN >= r.lsn) {
                skipped++;
                continue;
            }

            redoRecord(r);
            redone++;
        }

        // UNDO: roll back incomplete (crashed mid-flight) transactions
        int undone = 0;
        for (int i = records.size() - 1; i >= 0; i--) {
            WALRecord r = records.get(i);
            if (committed.contains(r.txnId) || aborted.contains(r.txnId)) continue;
            if (r.type != WALRecord.Type.INSERT &&
                r.type != WALRecord.Type.UPDATE &&
                r.type != WALRecord.Type.DELETE) continue;
            undoRecord(r);
            undone++;
        }

        System.out.printf("WAL: recovery complete — redone=%d skipped=%d undone=%d%n",
            redone, skipped, undone);

        bpm.flushAllPages();
        wal.truncate();
    }

    // ── Redo / Undo ───────────────────────────────────────────────

    private void redoRecord(WALRecord r) throws IOException {
        if (r.tableId == null) return;
        PageId pid  = new PageId(r.tableId, r.pageNum);
        Page   page = bpm.fetchPage(pid);

        switch (r.type) {
            case INSERT -> page.insertRecord(r.afterImage);
            case UPDATE -> page.updateRecord(r.slot, r.afterImage);
            case DELETE -> page.deleteRecord(r.slot);
            default     -> {}
        }
        page.setPageLSN(r.lsn);
        bpm.unpinPage(pid, true);
    }

    private void undoRecord(WALRecord r) throws IOException {
        if (r.tableId == null) return;
        PageId pid  = new PageId(r.tableId, r.pageNum);
        Page   page = bpm.fetchPage(pid);

        switch (r.type) {
            case INSERT -> page.deleteRecord(r.slot);
            case UPDATE -> page.updateRecord(r.slot, r.beforeImage);
            case DELETE -> page.insertRecord(r.beforeImage);
            default     -> {}
        }
        bpm.unpinPage(pid, true);
    }

    public int        getActiveTxnCount() { return activeTxns.size(); }
    public WALManager getWAL()            { return wal; }
}
