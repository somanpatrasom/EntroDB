package com.entrodb.server;

import com.entrodb.buffer.BufferPoolManager;
import com.entrodb.catalog.CatalogManager;
import com.entrodb.index.IndexManager;
import com.entrodb.transaction.TransactionManager;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class StatsCollector {

    private final BufferPoolManager  bpm;
    private final CatalogManager     catalog;
    private final IndexManager       indexManager;
    private final TransactionManager txnManager;
    private final long               startTime;

    private final AtomicLong totalQueries  = new AtomicLong(0);
    private final AtomicLong totalErrors   = new AtomicLong(0);
    private final AtomicLong activeClients = new AtomicLong(0);

    private final ConcurrentLinkedDeque<QueryLog> recentQueries
        = new ConcurrentLinkedDeque<>();

    public record QueryLog(String sql, long durationMs,
                            boolean error, String user, long timestamp) {}

    public StatsCollector(BufferPoolManager bpm, CatalogManager catalog,
                           IndexManager indexManager,
                           TransactionManager txnManager) {
        this.bpm          = bpm;
        this.catalog      = catalog;
        this.indexManager = indexManager;
        this.txnManager   = txnManager;
        this.startTime    = System.currentTimeMillis();
    }

    public void recordQuery(String sql, long durationMs,
                             boolean error, String user) {
        totalQueries.incrementAndGet();
        if (error) totalErrors.incrementAndGet();
        QueryLog log = new QueryLog(
            sql.length() > 80 ? sql.substring(0, 80) + "..." : sql,
            durationMs, error, user, System.currentTimeMillis());
        recentQueries.addFirst(log);
        while (recentQueries.size() > 20) recentQueries.pollLast();
    }

    public void clientConnected()    { activeClients.incrementAndGet(); }
    public void clientDisconnected() { activeClients.decrementAndGet(); }

    public Map<String, Object> snapshot() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("uptime_seconds",
            (System.currentTimeMillis() - startTime) / 1000);
        s.put("total_queries",  totalQueries.get());
        s.put("total_errors",   totalErrors.get());
        s.put("active_clients", activeClients.get());
        s.put("active_txns",    txnManager.getActiveTxnCount());
        s.put("active_locks",   txnManager.getLockManager().getActiveLockCount());
        s.put("buffer_pool_size", bpm.getPoolSize());
        s.put("buffer_pool_used", bpm.getUsedFrames());
        s.put("buffer_dirty",     bpm.getDirtyCount());

        List<Map<String, Object>> tables = new ArrayList<>();
        for (var schema : catalog.getAllTables()) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("name",    schema.getTableName());
            t.put("columns", schema.getColumns().size());
            t.put("indexes", indexManager
                .getIndexesForTable(schema.getTableName()).size());
            tables.add(t);
        }
        s.put("tables", tables);

        List<Map<String, Object>> queries = new ArrayList<>();
        for (QueryLog q : recentQueries) {
            Map<String, Object> qm = new LinkedHashMap<>();
            qm.put("sql",   q.sql());
            qm.put("ms",    q.durationMs());
            qm.put("error", q.error());
            qm.put("user",  q.user());
            qm.put("time",  q.timestamp());
            queries.add(qm);
        }
        s.put("recent_queries", queries);
        return s;
    }
}
