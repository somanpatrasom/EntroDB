package com.entrodb.buffer;

import com.entrodb.storage.PageId;
import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * LRU page replacement policy.
 * Evicts the least-recently-used unpinned page when the buffer is full.
 */
public class LRUReplacer {

    // LinkedHashMap with access-order = true gives us LRU for free
    private final LinkedHashMap<PageId, Boolean> lruMap;
    private final int capacity;

    public LRUReplacer(int capacity) {
        this.capacity = capacity;
        this.lruMap = new LinkedHashMap<>(capacity, 0.75f, true);
    }

    /** Record access (page is being used — move to MRU end). */
    public synchronized void recordAccess(PageId pid) {
        lruMap.put(pid, true);
    }

    /** Remove from LRU tracking (page is pinned — don't evict). */
    public synchronized void pin(PageId pid) {
        lruMap.remove(pid);
    }

    /** Add back to LRU tracking when unpinned. */
    public synchronized void unpin(PageId pid) {
        lruMap.put(pid, true);
    }

    /** Pick the LRU victim to evict. Returns empty if all pinned. */
    public synchronized Optional<PageId> evict() {
        if (lruMap.isEmpty()) return Optional.empty();
        PageId victim = lruMap.entrySet().iterator().next().getKey();
        lruMap.remove(victim);
        return Optional.of(victim);
    }

    public synchronized int size() { return lruMap.size(); }
}
