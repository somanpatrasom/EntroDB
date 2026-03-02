package com.nexusdb.storage;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles all disk I/O. Each table gets its own .ndb file.
 * Uses Java NIO FileChannel for direct, efficient I/O.
 */
public class DiskManager implements Closeable {

    private final Path dataDirectory;
    private final ConcurrentHashMap<String, FileChannel> openFiles;
    private final ConcurrentHashMap<String, Integer> pageCountCache;

    public DiskManager(String dataDir) throws IOException {
        this.dataDirectory = Paths.get(dataDir);
        Files.createDirectories(dataDirectory);
        this.openFiles = new ConcurrentHashMap<>();
        this.pageCountCache = new ConcurrentHashMap<>();
    }

    // ── Page I/O ──────────────────────────────────────────────────

    public void writePage(Page page) throws IOException {
        PageId pid = page.getPageId();
        FileChannel ch = getChannel(pid.tableId());
        long offset = (long) pid.pageNumber() * Page.PAGE_SIZE;

        ByteBuffer buf = ByteBuffer.wrap(page.getData());
        ch.write(buf, offset);
        ch.force(false); // flush to OS (not full fsync — WAL handles durability)
        page.setDirty(false);
    }

    public Page readPage(PageId pid) throws IOException {
        FileChannel ch = getChannel(pid.tableId());
        long offset = (long) pid.pageNumber() * Page.PAGE_SIZE;

        byte[] data = new byte[Page.PAGE_SIZE];
        ByteBuffer buf = ByteBuffer.wrap(data);

        int bytesRead = ch.read(buf, offset);
        if (bytesRead < Page.PAGE_SIZE) {
            // New or truncated page — zero fill
            Arrays.fill(data, bytesRead < 0 ? 0 : bytesRead, Page.PAGE_SIZE, (byte) 0);
        }
        return new Page(pid, data);
    }

    /** Allocate a new page at the end of the table file. Returns its number. */
    public int allocatePage(String tableId) throws IOException {
        FileChannel ch = getChannel(tableId);
        long size = ch.size();
        int newPageNum = (int) (size / Page.PAGE_SIZE);

        ByteBuffer zeroBuf = ByteBuffer.allocate(Page.PAGE_SIZE);
        ch.write(zeroBuf, (long) newPageNum * Page.PAGE_SIZE);
        ch.force(false);

        return newPageNum;
    }

    public int getPageCount(String tableId) throws IOException {
        FileChannel ch = getChannel(tableId);
        return (int) (ch.size() / Page.PAGE_SIZE);
    }

    public boolean tableExists(String tableId) {
        return Files.exists(dataDirectory.resolve(tableId + ".ndb"));
    }

    public void createTable(String tableId) throws IOException {
        Path file = dataDirectory.resolve(tableId + ".ndb");
        if (!Files.exists(file)) Files.createFile(file);
        getChannel(tableId); // open it
    }

    public void dropTable(String tableId) throws IOException {
        FileChannel ch = openFiles.remove(tableId);
        if (ch != null) ch.close();
        pageCountCache.remove(tableId);
        Files.deleteIfExists(dataDirectory.resolve(tableId + ".ndb"));
    }

    // ── Internal ──────────────────────────────────────────────────

    private FileChannel getChannel(String tableId) throws IOException {
        return openFiles.computeIfAbsent(tableId, id -> {
            try {
                Path file = dataDirectory.resolve(id + ".ndb");
                return FileChannel.open(file,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        for (FileChannel ch : openFiles.values()) ch.close();
        openFiles.clear();
    }
}
