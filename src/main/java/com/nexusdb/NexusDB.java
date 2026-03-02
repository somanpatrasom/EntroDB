package com.nexusdb;

import com.nexusdb.buffer.BufferPoolManager;
import com.nexusdb.catalog.CatalogManager;
import com.nexusdb.cli.REPL;
import com.nexusdb.executor.QueryExecutor;
import com.nexusdb.index.IndexManager;
import com.nexusdb.server.NexusServer;
import com.nexusdb.storage.DiskManager;
import com.nexusdb.transaction.TransactionManager;
import com.nexusdb.transaction.WALManager;

public class NexusDB {

    private static final String DATA_DIR         = "data/";
    private static final int    BUFFER_POOL_SIZE = 256;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting NexusDB...");

        // Boot storage layer
        DiskManager       disk    = new DiskManager(DATA_DIR);
        BufferPoolManager bpm     = new BufferPoolManager(BUFFER_POOL_SIZE, disk);
        CatalogManager    catalog = new CatalogManager(DATA_DIR);

        // Boot WAL + transactions
        WALManager         wal    = new WALManager(DATA_DIR);
        TransactionManager txnMgr = new TransactionManager(wal, bpm, catalog);

        // Boot index manager — load existing indexes for all known tables
        IndexManager indexManager = new IndexManager(DATA_DIR);
        for (var schema : catalog.getAllTables())
            indexManager.initTable(schema);

        // Crash recovery
        txnMgr.recover();

        // Boot executor
        QueryExecutor executor = new QueryExecutor(bpm, catalog, txnMgr, indexManager);

        boolean serverMode = args.length > 0 && args[0].equals("--server");
        int     port       = args.length > 1 ? Integer.parseInt(args[1])
                                              : NexusServer.DEFAULT_PORT;

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nCheckpointing...");
                bpm.flushAllPages();
                indexManager.flushAll();
                wal.close();
                disk.close();
                System.out.println("NexusDB shutdown complete.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        if (serverMode) {
            NexusServer server = new NexusServer(port, executor);
            Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
            server.start();
        } else {
            System.out.println("Starting NexusDB...");
            new REPL(executor).start();
        }
    }
}
