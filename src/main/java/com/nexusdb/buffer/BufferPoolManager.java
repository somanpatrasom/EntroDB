package com.nexusdb.buffer;

import com.nexusdb.storage.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The Buffer Pool sits between every component and the disk.
 * It holds a fixed number of frames (pages in RAM).
 *
 * Workflow:
 *  1. fetchPage() — check frame table, return if cached, else load from disk
 *  2. unpin()     — caller releases the page
 *  3. flushPage() — write dirty pages back to disk
 */
public class BufferPoolManager {

    private final int poolSize;
    private final Page[] frames;             // the actual page frames
    private final Map<PageId, Integer> pageTable;  // PageId → frame index
    private final boolean[] freeFrames;
    private final LRUReplacer replacer;
    private final DiskManager diskManager;

    public BufferPoolManager(int poolSize, DiskManager diskManager) {
        this.poolSize = poolSize;
        this.diskManager = diskManager;
        this.frames = new Page[poolSize];
        this.pageTable = new HashMap<>();
        this.freeFrames = new boolean[poolSize];
        this.replacer = new LRUReplacer(poolSize);

        for (int i = 0; i < poolSize; i++) freeFrames[i] = true; // all free
    }

    /**
     * Fetch a page into the buffer pool (or return it if already there).
     * Caller MUST call unpin() when done.
     */
    public synchronized Page fetchPage(PageId pid) throws IOException {
        // Cache hit
        if (pageTable.containsKey(pid)) {
            int frame = pageTable.get(pid);
            frames[frame].pin();
            replacer.pin(pid);
            return frames[frame];
        }

        // Find a free frame or evict
        int frame = findFreeFrame();
        if (frame == -1) {
            frame = evictPage();
            if (frame == -1) throw new IOException("Buffer pool full — all pages pinned");
        }

        // Load page from disk
        Page page = diskManager.readPage(pid);
        page.pin();
        frames[frame] = page;
        pageTable.put(pid, frame);
        freeFrames[frame] = false;
        replacer.pin(pid);

        return page;
    }

    /** Allocate a brand new page (on disk + in buffer). */
    public synchronized Page newPage(String tableId) throws IOException {
        int pageNum = diskManager.allocatePage(tableId);
        PageId pid = new PageId(tableId, pageNum);

        int frame = findFreeFrame();
        if (frame == -1) {
            frame = evictPage();
            if (frame == -1) throw new IOException("Buffer pool full");
        }

        Page page = new Page(pid);
        page.pin();
        frames[frame] = page;
        pageTable.put(pid, frame);
        freeFrames[frame] = false;
        replacer.pin(pid);

        return page;
    }

    /** Release a page. Set dirty=true if you modified it. */
    public synchronized void unpinPage(PageId pid, boolean dirty) throws IOException {
        if (!pageTable.containsKey(pid)) return;
        int frame = pageTable.get(pid);
        Page page = frames[frame];
        if (dirty) {
            page.setDirty(true);
            diskManager.writePage(page);  // write-through: flush immediately
            page.setDirty(false);
        }
        page.unpin();
        if (page.getPinCount() == 0) replacer.unpin(pid);
    }

    /** Write a specific dirty page back to disk. */
    public synchronized void flushPage(PageId pid) throws IOException {
        if (!pageTable.containsKey(pid)) return;
        Page page = frames[pageTable.get(pid)];
        if (page.isDirty()) diskManager.writePage(page);
    }

    /** Flush ALL dirty pages (called on shutdown or checkpoint). */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : frames) {
            if (page != null && page.isDirty()) diskManager.writePage(page);
        }
    }

    // ── Internal helpers ──────────────────────────────────────────

    private int findFreeFrame() {
        for (int i = 0; i < poolSize; i++) {
            if (freeFrames[i]) return i;
        }
        return -1;
    }

    private int evictPage() throws IOException {
        var victim = replacer.evict();
        if (victim.isEmpty()) return -1;

        PageId pid = victim.get();
        int frame = pageTable.get(pid);
        Page page = frames[frame];

        if (page.isDirty()) diskManager.writePage(page);

        pageTable.remove(pid);
        freeFrames[frame] = true;
        frames[frame] = null;
        return frame;
    }

    public DiskManager getDiskManager() { return diskManager; }
}
