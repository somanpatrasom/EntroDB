package com.entrodb.transaction;

/**
 * A single entry in the Write-Ahead Log.
 *
 * Format on disk:
 * [LSN:8][txnId:8][type:1][tableId length:2][tableId bytes]
 * [pageNum:4][slot:4][dataLength:4][data bytes][checksum:4]
 *
 * LSN = Log Sequence Number (monotonically increasing)
 */
public class WALRecord {

    public enum Type {
        BEGIN,      // transaction started
        INSERT,     // row inserted
        UPDATE,     // row updated (stores before-image + after-image)
        DELETE,     // row deleted (stores before-image for undo)
        COMMIT,     // transaction committed
        ABORT,      // transaction aborted
        CHECKPOINT  // all dirty pages flushed to disk
    }

    public final long   lsn;
    public final long   txnId;
    public final Type   type;
    public final String tableId;
    public final int    pageNum;
    public final int    slot;
    public final byte[] beforeImage; // for UPDATE and DELETE (undo)
    public final byte[] afterImage;  // for INSERT and UPDATE (redo)

    // For BEGIN, COMMIT, ABORT, CHECKPOINT
    public WALRecord(long lsn, long txnId, Type type) {
        this(lsn, txnId, type, null, -1, -1, null, null);
    }

    public WALRecord(long lsn, long txnId, Type type,
                     String tableId, int pageNum, int slot,
                     byte[] beforeImage, byte[] afterImage) {
        this.lsn         = lsn;
        this.txnId       = txnId;
        this.type        = type;
        this.tableId     = tableId;
        this.pageNum     = pageNum;
        this.slot        = slot;
        this.beforeImage = beforeImage;
        this.afterImage  = afterImage;
    }

    @Override
    public String toString() {
        return String.format("WALRecord[lsn=%d txn=%d type=%s table=%s page=%d slot=%d]",
            lsn, txnId, type, tableId, pageNum, slot);
    }
}
