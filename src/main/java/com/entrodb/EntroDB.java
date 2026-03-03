package com.entrodb;

import com.entrodb.buffer.BufferPoolManager;
import com.entrodb.catalog.CatalogManager;
import com.entrodb.cli.REPL;
import com.entrodb.executor.QueryExecutor;
import com.entrodb.index.IndexManager;
import com.entrodb.server.*;
import com.entrodb.storage.DiskManager;
import com.entrodb.transaction.TransactionManager;
import com.entrodb.transaction.WALManager;

public class EntroDB {

    private static final String DATA_DIR         = "data/";
    private static final int    BUFFER_POOL_SIZE = 256;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting EntroDB...");

        DiskManager        disk    = new DiskManager(DATA_DIR);
        BufferPoolManager  bpm     = new BufferPoolManager(BUFFER_POOL_SIZE, disk);
        CatalogManager     catalog = new CatalogManager(DATA_DIR);
        WALManager         wal     = new WALManager(DATA_DIR);
        TransactionManager txnMgr  = new TransactionManager(wal, bpm, catalog);

        IndexManager indexManager = new IndexManager(DATA_DIR);
        for (var schema : catalog.getAllTables())
            indexManager.initTable(schema);

        txnMgr.recover();

        AuthManager   auth     = new AuthManager(DATA_DIR);
        StatsCollector stats   = new StatsCollector(bpm, catalog,
                                                     indexManager, txnMgr);
        QueryExecutor executor = new QueryExecutor(bpm, catalog,
                                                    txnMgr, indexManager);

        boolean serverMode = args.length > 0 && args[0].equals("--server");
        int     port       = args.length > 1
                           ? Integer.parseInt(args[1])
                           : EntroServer.DEFAULT_PORT;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nCheckpointing...");
                bpm.flushAllPages();
                indexManager.flushAll();
                wal.close();
                disk.close();
                System.out.println("EntroDB shutdown complete.");
            } catch (Exception e) { e.printStackTrace(); }
        }));

        if (serverMode) {
            DashboardServer dashboard = new DashboardServer(
                DashboardServer.DEFAULT_DASHBOARD_PORT, stats);
            dashboard.start();

            EntroServer server = new EntroServer(port, executor, auth, stats);
            Runtime.getRuntime().addShutdownHook(
                new Thread(server::stop));
            server.start();
        } else {
            new REPL(executor).start();
        }
    }
}
