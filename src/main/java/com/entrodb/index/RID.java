package com.entrodb.index;

/**
 * Record Identifier — points to a specific record on disk.
 * Stored as the value in B+ Tree leaf nodes.
 */
public record RID(int pageNum, int slot) {

    @Override
    public String toString() {
        return pageNum + ":" + slot;
    }

    public static RID fromString(String s) {
        String[] parts = s.split(":");
        return new RID(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    /** Serialize to 8 bytes: [pageNum:4][slot:4] */
    public byte[] toBytes() {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8);
        buf.putInt(pageNum);
        buf.putInt(slot);
        return buf.array();
    }

    public static RID fromBytes(byte[] b) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b);
        return new RID(buf.getInt(), buf.getInt());
    }
}
