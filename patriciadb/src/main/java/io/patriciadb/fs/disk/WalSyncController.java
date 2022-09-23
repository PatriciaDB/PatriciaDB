package io.patriciadb.fs.disk;

import io.patriciadb.Storage;
import io.patriciadb.fs.disk.datastorage.disk.AppenderDataStorage;
import io.patriciadb.fs.disk.directory.wal.WalDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class WalSyncController {

    private final static Logger log = LoggerFactory.getLogger(WalSyncController.class);
    private final WalDirectory walDirectory;
    private final AppenderDataStorage storage;
    private final AtomicBoolean syncRequest = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<CompletableFuture<Void>> callbacks = new ConcurrentLinkedQueue<>();
    private final Consumer<Throwable> exceptionHandler;
    private final Executor callbackExecutor;

    public WalSyncController(WalDirectory walDirectory, AppenderDataStorage storage,Consumer<Throwable> exceptionHandler, Executor callbackExecutor) {
        this.walDirectory = walDirectory;
        this.storage = storage;
        this.exceptionHandler = exceptionHandler;
        this.callbackExecutor = callbackExecutor;
    }

    public void syncWalJob() {
        if(!syncRequest.compareAndSet(true, false)) {
            return;
        }
        log.trace("Wal Sync Started");
        var callbackCopy = new ArrayList<CompletableFuture<Void>>();
        while(!callbacks.isEmpty()) {
            callbackCopy.add(callbacks.poll());
        }
        try {
            long startTime = System.currentTimeMillis();
            syncWal();
            log.trace("Wal Synced in {}ms", System.currentTimeMillis()-startTime);
            for(var callback: callbackCopy) {
                callback.completeAsync(() ->null, callbackExecutor);
            }
        } catch (Throwable t) {
            log.warn("Error while syncing wal files", t);
            if(exceptionHandler!=null) {
                exceptionHandler.accept(t);
            }
        }

    }

    public void syncWal() {
        storage.flushAndSync();
        walDirectory.sync();
    }

    public void setSyncRequest() {
        syncRequest.set(true);
    }

    public CompletableFuture<Void> registerCallback() {
        var cf = new CompletableFuture<Void>();
        callbacks.add(cf);
        return cf;
    }
}
