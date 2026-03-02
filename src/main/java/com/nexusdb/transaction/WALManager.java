package com.nexusdb.transaction;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Manages the Write-Ahead Log file: data/nexus.wal
 *
 * Guarantees:
 *  - Every WAL record is fsync'd before the corresponding data page write
 *  - On startup, replays any committed but unwritten transactions
 *  - On startup, rolls back any uncommitted transactions
 */
public class WALManager implements Closeable {

    private static final String WAL_FILE      = "nexus.wal";
    private static final int    MAGIC         = 0x4E455857; // "NEXW"
    private static final int    HEADER_SIZE   = 4 + 8 + 1 + 2; // magic+lsn+type+tableLen

    private final Path          walPath;
    private final FileChannel   channel;
    private final AtomicLong    lsnCounter;

    public WALManager(String dataDir) throws IOException {
        this.walPath    = Paths.get(dataDir, WAL_FILE);
        this.channel    = FileChannel.open(walPath,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);
        this.lsnCounter = new AtomicLong(channel.size() > 0 ? getLastLSN() : 0);
    }

    // ── Writing ───────────────────────────────────────────────────

    public synchronized long append(WALRecord record) throws IOException {
        byte[] encoded = encode(record);
        ByteBuffer buf = ByteBuffer.wrap(encoded);
        channel.write(buf, channel.size());
        channel.force(true); // fsync — critical for durability
        return record.lsn;
    }

    public long nextLSN() {
        return lsnCounter.incrementAndGet();
    }

    // ── Recovery ──────────────────────────────────────────────────

    /**
     * Read all WAL records. Called on startup for recovery.
     */
    public List<WALRecord> readAll() throws IOException {
        List<WALRecord> records = new ArrayList<>();
        channel.position(0);

        while (channel.position() < channel.size()) {
            try {
                WALRecord record = decodeNext();
                if (record != null) records.add(record);
            } catch (IOException e) {
                // Truncated record at end — stop reading
                System.err.println("WAL: truncated record at offset "
                    + channel.position() + ", stopping recovery read");
                break;
            }
        }
        return records;
    }

    /**
     * Truncate the WAL after successful checkpoint.
     * All committed transactions have been flushed to data files.
     */
    public synchronized void truncate() throws IOException {
        channel.truncate(0);
        channel.force(true);
        System.out.println("WAL: truncated after checkpoint");
    }

    // ── Encoding ──────────────────────────────────────────────────

    private byte[] encode(WALRecord r) {
        byte[] tableBytes    = r.tableId    != null ? r.tableId.getBytes()    : new byte[0];
        byte[] beforeBytes   = r.beforeImage != null ? r.beforeImage          : new byte[0];
        byte[] afterBytes    = r.afterImage  != null ? r.afterImage           : new byte[0];

        int size = 4           // MAGIC
                 + 8           // LSN
                 + 8           // txnId
                 + 1           // type
                 + 2           // tableId length
                 + tableBytes.length
                 + 4           // pageNum
                 + 4           // slot
                 + 4           // beforeImage length
                 + beforeBytes.length
                 + 4           // afterImage length
                 + afterBytes.length
                 + 4;          // CRC32 checksum

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(MAGIC);
        buf.putLong(r.lsn);
        buf.putLong(r.txnId);
        buf.put((byte) r.type.ordinal());
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);
        buf.putInt(r.pageNum);
        buf.putInt(r.slot);
        buf.putInt(beforeBytes.length);
        buf.put(beforeBytes);
        buf.putInt(afterBytes.length);
        buf.put(afterBytes);

        // Checksum everything so far
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc.getValue());

        return buf.array();
    }

    private WALRecord decodeNext() throws IOException {
        // Read fixed header: MAGIC(4) + LSN(8) + txnId(8) + type(1) + tableLen(2) = 23
        ByteBuffer header = ByteBuffer.allocate(23);
        if (channel.read(header) < 23) throw new EOFException();
        header.flip();

        int magic = header.getInt();
        if (magic != MAGIC) throw new IOException("Invalid WAL magic: " + magic);

        long lsn        = header.getLong();
        long txnId      = header.getLong();
        WALRecord.Type type = WALRecord.Type.values()[header.get()];
        int  tableLen   = header.getShort() & 0xFFFF;

        String tableId = null;
        if (tableLen > 0) {
            ByteBuffer tb = ByteBuffer.allocate(tableLen);
            channel.read(tb);
            tableId = new String(tb.array());
        }

        // Read pageNum(4) + slot(4)
        ByteBuffer loc = ByteBuffer.allocate(8);
        channel.read(loc); loc.flip();
        int pageNum = loc.getInt();
        int slot    = loc.getInt();

        // Read before image
        ByteBuffer blen = ByteBuffer.allocate(4);
        channel.read(blen); blen.flip();
        int beforeLen = blen.getInt();
        byte[] beforeImage = new byte[beforeLen];
        if (beforeLen > 0) { ByteBuffer bb = ByteBuffer.wrap(beforeImage); channel.read(bb); }

        // Read after image
        ByteBuffer alen = ByteBuffer.allocate(4);
        channel.read(alen); alen.flip();
        int afterLen = alen.getInt();
        byte[] afterImage = new byte[afterLen];
        if (afterLen > 0) { ByteBuffer ab = ByteBuffer.wrap(afterImage); channel.read(ab); }

        // Skip checksum (4 bytes)
        channel.position(channel.position() + 4);

        return new WALRecord(lsn, txnId, type, tableId, pageNum, slot, beforeImage, afterImage);
    }

    private long getLastLSN() throws IOException {
        // Quick scan to find highest LSN
        List<WALRecord> records = readAll();
        return records.isEmpty() ? 0 : records.get(records.size() - 1).lsn;
    }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }
}
