package com.nexusdb.storage;

import java.nio.ByteBuffer;

/**
 * Page Layout (slotted page format):
 * [Header 24 bytes][Slot Array →][← Free Space][← Records]
 *
 * Header: [numSlots:4][freeSpaceOffset:4][pageNumber:4][flags:4][pageLSN:8]
 *
 * pageLSN = LSN of the last WAL record applied to this page.
 * Recovery skips any WAL record with LSN <= pageLSN.
 */
public class Page {

    public static final int PAGE_SIZE   = 4096;
    public static final int HEADER_SIZE = 24; // increased from 16 to fit pageLSN

    private final byte[] data;
    private PageId  pageId;
    private boolean dirty;
    private int     pinCount;

    public Page(PageId pageId) {
        this.pageId   = pageId;
        this.data     = new byte[PAGE_SIZE];
        this.dirty    = false;
        this.pinCount = 0;

        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.putInt(0);                   // numSlots = 0
        buf.putInt(PAGE_SIZE);           // freeSpaceOffset = end of page
        buf.putInt(pageId.pageNumber()); // pageNumber
        buf.putInt(0);                   // flags
        buf.putLong(0L);                 // pageLSN = 0
    }

    public Page(PageId pageId, byte[] rawData) {
        this.pageId   = pageId;
        this.data     = rawData.clone();
        this.dirty    = false;
        this.pinCount = 0;
    }

    // ── pageLSN ───────────────────────────────────────────────────

    public long getPageLSN() {
        return ByteBuffer.wrap(data).getLong(16); // offset 16 = after numSlots+freeSpace+pageNum+flags
    }

    public void setPageLSN(long lsn) {
        ByteBuffer.wrap(data).putLong(16, lsn);
        dirty = true;
    }

    // ── Slot-based record management ──────────────────────────────

    public int insertRecord(byte[] record) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int numSlots        = buf.getInt(0);
        int freeSpaceOffset = buf.getInt(4);

        int slotArrayEnd = HEADER_SIZE + (numSlots + 1) * 8;
        int recordStart  = freeSpaceOffset - record.length;

        if (recordStart < slotArrayEnd) return -1; // page full

        System.arraycopy(record, 0, data, recordStart, record.length);

        int slotOffset = HEADER_SIZE + numSlots * 8;
        buf.putInt(slotOffset,     recordStart);
        buf.putInt(slotOffset + 4, record.length);

        buf.putInt(0, numSlots + 1);
        buf.putInt(4, recordStart);

        dirty = true;
        return numSlots;
    }

    public byte[] readRecord(int slot) {
        ByteBuffer buf      = ByteBuffer.wrap(data);
        int        numSlots = buf.getInt(0);
        if (slot < 0 || slot >= numSlots) return null;

        int slotOffset = HEADER_SIZE + slot * 8;
        int recOffset  = buf.getInt(slotOffset);
        int recLength  = buf.getInt(slotOffset + 4);

        if (recOffset == 0 || recLength == 0) return null;

        byte[] record = new byte[recLength];
        System.arraycopy(data, recOffset, record, 0, recLength);
        return record;
    }

    public void deleteRecord(int slot) {
        ByteBuffer buf      = ByteBuffer.wrap(data);
        int        numSlots = buf.getInt(0);
        if (slot < 0 || slot >= numSlots) return;

        int slotOffset = HEADER_SIZE + slot * 8;
        buf.putInt(slotOffset,     0);
        buf.putInt(slotOffset + 4, 0);
        dirty = true;
    }

    public boolean updateRecord(int slot, byte[] newRecord) {
        ByteBuffer buf      = ByteBuffer.wrap(data);
        int        numSlots = buf.getInt(0);
        if (slot < 0 || slot >= numSlots) return false;

        int slotOffset = HEADER_SIZE + slot * 8;
        int recOffset  = buf.getInt(slotOffset);
        int recLength  = buf.getInt(slotOffset + 4);

        if (recOffset == 0 && recLength == 0) return false;

        if (newRecord.length <= recLength) {
            System.arraycopy(newRecord, 0, data, recOffset, newRecord.length);
            buf.putInt(slotOffset + 4, newRecord.length);
            dirty = true;
            return true;
        }
        return false;
    }

    public int getNumSlots() {
        return ByteBuffer.wrap(data).getInt(0);
    }

    public int getFreeSpace() {
        ByteBuffer buf            = ByteBuffer.wrap(data);
        int        freeSpaceOffset = buf.getInt(4);
        int        numSlots        = buf.getInt(0);
        int        slotArrayEnd    = HEADER_SIZE + (numSlots + 1) * 8;
        return freeSpaceOffset - slotArrayEnd;
    }

    public byte[]  getData()               { return data; }
    public PageId  getPageId()             { return pageId; }
    public boolean isDirty()               { return dirty; }
    public void    setDirty(boolean dirty) { this.dirty = dirty; }
    public int     getPinCount()           { return pinCount; }
    public void    pin()                   { pinCount++; }
    public void    unpin()                 { if (pinCount > 0) pinCount--; }
}
