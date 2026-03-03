package com.entrodb.util;

import com.entrodb.catalog.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Serializes/deserializes rows to/from byte arrays for storage in Pages.
 *
 * Row format:
 * [nullBitmap: ceil(cols/8) bytes]
 * [col0 bytes][col1 bytes]...[colN bytes]
 * For VARCHAR: [length:2][bytes]
 */
public class ByteUtils {

    public static byte[] serializeRow(List<Object> values, TableSchema schema) {
        List<Column> cols = schema.getColumns();
        int nullBitmapSize = (cols.size() + 7) / 8;
        byte[] nullBitmap = new byte[nullBitmapSize];

        List<byte[]> colBytes = new ArrayList<>();
        int totalSize = nullBitmapSize;

        for (int i = 0; i < cols.size(); i++) {
            Object val = values.get(i);
            if (val == null) {
                nullBitmap[i / 8] |= (1 << (i % 8));
                colBytes.add(new byte[0]);
            } else {
                byte[] b = encodeValue(val, cols.get(i).type());
                colBytes.add(b);
                totalSize += b.length;
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(nullBitmap);
        for (byte[] b : colBytes) buf.put(b);
        return buf.array();
    }

    public static List<Object> deserializeRow(byte[] data, TableSchema schema) {
        List<Column> cols = schema.getColumns();
        int nullBitmapSize = (cols.size() + 7) / 8;
        ByteBuffer buf = ByteBuffer.wrap(data);

        byte[] nullBitmap = new byte[nullBitmapSize];
        buf.get(nullBitmap);

        List<Object> values = new ArrayList<>();
        for (int i = 0; i < cols.size(); i++) {
            boolean isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
            if (isNull) { values.add(null); continue; }
            values.add(decodeValue(buf, cols.get(i).type()));
        }
        return values;
    }

    private static byte[] encodeValue(Object val, Column.DataType type) {
        return switch (type) {
            case INT -> ByteBuffer.allocate(4).putInt((Integer) val).array();
            case BIGINT -> ByteBuffer.allocate(8).putLong((Long) val).array();
            case FLOAT -> ByteBuffer.allocate(8).putDouble((Double) val).array();
            case BOOLEAN -> new byte[]{ (byte) ((Boolean) val ? 1 : 0) };
            case VARCHAR -> {
                byte[] bytes = val.toString().getBytes(StandardCharsets.UTF_8);
                ByteBuffer b = ByteBuffer.allocate(2 + bytes.length);
                b.putShort((short) bytes.length);
                b.put(bytes);
                yield b.array();
            }
        };
    }

    private static Object decodeValue(ByteBuffer buf, Column.DataType type) {
        return switch (type) {
            case INT -> buf.getInt();
            case BIGINT -> buf.getLong();
            case FLOAT -> buf.getDouble();
            case BOOLEAN -> buf.get() != 0;
            case VARCHAR -> {
                int len = buf.getShort() & 0xFFFF;
                byte[] bytes = new byte[len];
                buf.get(bytes);
                yield new String(bytes, StandardCharsets.UTF_8);
            }
        };
    }
}
