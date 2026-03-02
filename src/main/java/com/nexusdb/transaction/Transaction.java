package com.nexusdb.transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single database transaction.
 *
 * States:  ACTIVE → COMMITTED
 *          ACTIVE → ABORTED
 */
public class Transaction {

    public enum State { ACTIVE, COMMITTED, ABORTED }

    private final long        txnId;
    private       State       state;
    private final List<Long>  walLSNs;   // LSNs of all WAL records for this txn
    private final long        startTime;

    public Transaction(long txnId) {
        this.txnId     = txnId;
        this.state     = State.ACTIVE;
        this.walLSNs   = new ArrayList<>();
        this.startTime = System.currentTimeMillis();
    }

    public void addLSN(long lsn)     { walLSNs.add(lsn); }
    public void commit()             { state = State.COMMITTED; }
    public void abort()              { state = State.ABORTED; }

    public long        getTxnId()    { return txnId; }
    public State       getState()    { return state; }
    public List<Long>  getWALLSNs() { return walLSNs; }
    public boolean     isActive()    { return state == State.ACTIVE; }

    public String getSummary() {
        long ms = System.currentTimeMillis() - startTime;
        return String.format("Transaction[id=%d state=%s lsnCount=%d age=%dms]",
            txnId, state, walLSNs.size(), ms);
    }
}
