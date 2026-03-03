package com.entrodb.index;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class BPlusTree {

    public static final int ORDER = 128;

    private BPlusTreeNode root;
    private final Path    indexFile;
    private int           size;

    @SuppressWarnings("unchecked")
    private static final Comparator<Comparable> KEY_CMP =
        (a, b) -> a.compareTo(b);

    public BPlusTree(String indexFilePath) throws IOException {
        this.indexFile = Paths.get(indexFilePath);
        if (Files.exists(indexFile) && Files.size(indexFile) > 0) {
            load();
        } else {
            this.root = new BPlusTreeNode(BPlusTreeNode.Type.LEAF);
            this.size = 0;
        }
    }

    // ── Public API ────────────────────────────────────────────────

    public void insert(Comparable key, RID rid) {
        InsertResult result = insertIntoNode(root, key, rid);
        if (result != null) {
            BPlusTreeNode newRoot = new BPlusTreeNode(BPlusTreeNode.Type.INTERNAL);
            newRoot.keys.add(result.promotedKey);
            newRoot.children.add(root);
            newRoot.children.add(result.newNode);
            root = newRoot;
        }
        size++;
    }

    public RID search(Comparable key) {
        BPlusTreeNode leaf = findLeaf(key);
        int idx = binarySearch(leaf.keys, key);
        if (idx < 0) return null;
        return leaf.rids.get(idx);
    }

    public List<RID> rangeSearch(Comparable low, Comparable high) {
        List<RID> results = new ArrayList<>();
        BPlusTreeNode leaf = findLeaf(low);
        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                int cmpLow  = KEY_CMP.compare(leaf.keys.get(i), low);
                int cmpHigh = KEY_CMP.compare(leaf.keys.get(i), high);
                if (cmpLow >= 0 && cmpHigh <= 0) results.add(leaf.rids.get(i));
                if (cmpHigh > 0) return results;
            }
            leaf = leaf.next;
        }
        return results;
    }

    public void update(Comparable key, RID newRid) {
        BPlusTreeNode leaf = findLeaf(key);
        int idx = binarySearch(leaf.keys, key);
        if (idx >= 0) leaf.rids.set(idx, newRid);
    }

    public void delete(Comparable key) {
        deleteFromLeaf(root, key);
        size = Math.max(0, size - 1);
    }

    public int     size()    { return size; }
    public boolean isEmpty() { return size == 0; }

    // ── Insert internals ──────────────────────────────────────────

    private InsertResult insertIntoNode(BPlusTreeNode node, Comparable key, RID rid) {
        if (node.isLeaf()) return insertIntoLeaf(node, key, rid);

        int childIdx = findChildIndex(node, key);
        InsertResult result = insertIntoNode(node.children.get(childIdx), key, rid);
        if (result == null) return null;

        int insertPos = findInsertPosition(node.keys, result.promotedKey);
        node.keys.add(insertPos, result.promotedKey);
        node.children.add(insertPos + 1, result.newNode);

        if (node.keys.size() < 2 * ORDER) return null;
        return splitInternal(node);
    }

    private InsertResult insertIntoLeaf(BPlusTreeNode leaf, Comparable key, RID rid) {
        int pos = findInsertPosition(leaf.keys, key);

        if (pos < leaf.keys.size() && KEY_CMP.compare(leaf.keys.get(pos), key) == 0)
            throw new RuntimeException("Duplicate key in index: " + key);

        leaf.keys.add(pos, key);
        leaf.rids.add(pos, rid);
        leaf.dirty = true;

        if (leaf.keys.size() < 2 * ORDER) return null;
        return splitLeaf(leaf);
    }

    private InsertResult splitLeaf(BPlusTreeNode leaf) {
        int mid = leaf.keys.size() / 2;

        BPlusTreeNode newLeaf = new BPlusTreeNode(BPlusTreeNode.Type.LEAF);
        newLeaf.keys.addAll(new ArrayList<>(leaf.keys.subList(mid, leaf.keys.size())));
        newLeaf.rids.addAll(new ArrayList<>(leaf.rids.subList(mid, leaf.rids.size())));

        leaf.keys.subList(mid, leaf.keys.size()).clear();
        leaf.rids.subList(mid, leaf.rids.size()).clear();

        newLeaf.next = leaf.next;
        leaf.next    = newLeaf;
        leaf.dirty   = true;

        return new InsertResult(newLeaf.keys.get(0), newLeaf);
    }

    private InsertResult splitInternal(BPlusTreeNode node) {
        int mid = node.keys.size() / 2;
        Comparable promotedKey = node.keys.get(mid);

        BPlusTreeNode newNode = new BPlusTreeNode(BPlusTreeNode.Type.INTERNAL);
        newNode.keys.addAll(new ArrayList<>(node.keys.subList(mid + 1, node.keys.size())));
        newNode.children.addAll(new ArrayList<>(node.children.subList(mid + 1, node.children.size())));

        node.keys.subList(mid, node.keys.size()).clear();
        node.children.subList(mid + 1, node.children.size()).clear();
        node.dirty = true;

        return new InsertResult(promotedKey, newNode);
    }

    // ── Delete internals ──────────────────────────────────────────

    private void deleteFromLeaf(BPlusTreeNode node, Comparable key) {
        if (node.isLeaf()) {
            int idx = binarySearch(node.keys, key);
            if (idx >= 0) {
                node.keys.remove(idx);
                node.rids.remove(idx);
                node.dirty = true;
            }
            return;
        }
        int childIdx = findChildIndex(node, key);
        deleteFromLeaf(node.children.get(childIdx), key);
    }

    // ── Search helpers ────────────────────────────────────────────

    private BPlusTreeNode findLeaf(Comparable key) {
        BPlusTreeNode node = root;
        while (node.isInternal()) {
            node = node.children.get(findChildIndex(node, key));
        }
        return node;
    }

    private int findChildIndex(BPlusTreeNode node, Comparable key) {
        int i = 0;
        while (i < node.keys.size() && KEY_CMP.compare(key, node.keys.get(i)) >= 0) i++;
        return i;
    }

    private int findInsertPosition(List<Comparable> keys, Comparable key) {
        int lo = 0, hi = keys.size();
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if (KEY_CMP.compare(keys.get(mid), key) <= 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    // Replace Collections.binarySearch with explicit comparator version
    private int binarySearch(List<Comparable> keys, Comparable key) {
        int lo = 0, hi = keys.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            int cmp = KEY_CMP.compare(keys.get(mid), key);
            if      (cmp == 0) return mid;
            else if (cmp < 0)  lo = mid + 1;
            else               hi = mid - 1;
        }
        return -1; // not found
    }

    // ── Persistence ───────────────────────────────────────────────

    public void save() throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(indexFile)) {
            w.write("SIZE:" + size + "\n");
            Queue<BPlusTreeNode> queue = new LinkedList<>();
            queue.add(root);
            while (!queue.isEmpty()) {
                BPlusTreeNode node = queue.poll();
                if (node.isInternal()) {
                    StringBuilder sb = new StringBuilder("I|");
                    for (int i = 0; i < node.keys.size(); i++) {
                        if (i > 0) sb.append(",");
                        sb.append(node.keys.get(i));
                    }
                    w.write(sb + "\n");
                    for (BPlusTreeNode child : node.children) queue.add(child);
                } else {
                    StringBuilder sb = new StringBuilder("L|");
                    for (int i = 0; i < node.keys.size(); i++) {
                        if (i > 0) sb.append(",");
                        sb.append(node.keys.get(i))
                          .append(":").append(node.rids.get(i).pageNum())
                          .append(":").append(node.rids.get(i).slot());
                    }
                    w.write(sb + "\n");
                }
            }
        }
    }

    private void load() throws IOException {
        List<String> lines = Files.readAllLines(indexFile);
        if (lines.isEmpty()) {
            root = new BPlusTreeNode(BPlusTreeNode.Type.LEAF);
            return;
        }

        root = new BPlusTreeNode(BPlusTreeNode.Type.LEAF);
        size = 0;

        int lineIdx = 0;
        if (lines.get(0).startsWith("SIZE:")) lineIdx = 1;

        for (int i = lineIdx; i < lines.size(); i++) {
            String line = lines.get(i);
            if (!line.startsWith("L|")) continue;
            String payload = line.substring(2);
            if (payload.isEmpty()) continue;
            for (String entry : payload.split(",")) {
                String[] parts = entry.split(":");
                if (parts.length < 3) continue;
                Comparable key = parseKey(parts[0]);
                RID rid = new RID(Integer.parseInt(parts[1]),
                                  Integer.parseInt(parts[2]));
                insert(key, rid);
            }
        }
    }

    private Comparable parseKey(String s) {
        try { return Integer.parseInt(s); }  catch (NumberFormatException ignored) {}
        try { return Long.parseLong(s); }    catch (NumberFormatException ignored) {}
        try { return Double.parseDouble(s); } catch (NumberFormatException ignored) {}
        return s;
    }

    // ── Inner helper ──────────────────────────────────────────────

    private static class InsertResult {
        final Comparable    promotedKey;
        final BPlusTreeNode newNode;
        InsertResult(Comparable promotedKey, BPlusTreeNode newNode) {
            this.promotedKey = promotedKey;
            this.newNode     = newNode;
        }
    }
}
