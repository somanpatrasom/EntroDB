package com.entrodb.index;

import java.util.ArrayList;
import java.util.List;

/**
 * A node in the B+ Tree.
 *
 * INTERNAL node: keys=[k1,k2,...], children=[c0,c1,c2,...]
 *   children[i] contains keys < keys[i]
 *   children[last] contains keys >= keys[last-1]
 *
 * LEAF node: keys=[k1,k2,...], rids=[r1,r2,...]
 *   keys[i] maps to rids[i]
 *   leaf nodes are linked: next → next leaf
 */
public class BPlusTreeNode {

    public enum Type { INTERNAL, LEAF }

    final Type            type;
    final List<Comparable> keys;

    // Internal node fields
    final List<BPlusTreeNode> children;

    // Leaf node fields
    final List<RID>       rids;
    BPlusTreeNode         next; // linked list of leaves

    // Node is modified and needs persisting
    boolean dirty;

    public BPlusTreeNode(Type type) {
        this.type     = type;
        this.keys     = new ArrayList<>();
        this.children = new ArrayList<>();
        this.rids     = new ArrayList<>();
        this.next     = null;
        this.dirty    = true;
    }

    public boolean isLeaf()     { return type == Type.LEAF; }
    public boolean isInternal() { return type == Type.INTERNAL; }
    public int     size()       { return keys.size(); }
}
