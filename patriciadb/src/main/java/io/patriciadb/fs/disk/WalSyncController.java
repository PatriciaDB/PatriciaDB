package io.patriciadb.fs.disk;

import io.patriciadb.fs.CallbackExecutor;
import io.patriciadb.fs.TaskScheduledExecutor;
import io.patriciadb.fs.disk.datastorage.disk.AppenderDataStorage;
import io.patriciadb.fs.disk.directory.imp.WriteAheadLogDirectory;
import io.patriciadb.utils.lifecycle.BeansHolder;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WalSyncController implements PatriciaController {

    private final static Logger log = LoggerFactory.getLogger(WalSyncController.class);
    private final WriteAheadLogDirectory walDirectory;
    private final AppenderDataStorage storage;
    private final AtomicBoolean syncRequest = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<CompletableFuture<Void>> callbacks = new ConcurrentLinkedQueue<>();
    private final CallbackExecutor callbackExecutor;
    private final TaskScheduledExecutor taskScheduledExecutor;
    private final BeansHolder beansHolder;

    public WalSyncController(BeansHolder beansHolder, WriteAheadLogDirectory walDirectory, AppenderDataStorage storage, TaskScheduledExecutor taskScheduledExecutor, CallbackExecutor callbackExecutor) {
        this.walDirectory = walDirectory;
        this.storage = storage;
        this.callbackExecutor =callbackExecutor;
        this.taskScheduledExecutor = taskScheduledExecutor;
        this.beansHolder= beansHolder;
    }

    @Override
    public void initialize() throws Exception {
        taskScheduledExecutor.getExecutorService().scheduleWithFixedDelay(this::syncWalJob, 100, 200, TimeUnit.MILLISECONDS);
    }

    public void syncWalJob() {
        if (!syncRequest.compareAndSet(true, false)) {
            return;
        }
        log.trace("Wal Sync Started");
        var callbackCopy = new ArrayList<CompletableFuture<Void>>();
        while (!callbacks.isEmpty()) {
            callbackCopy.add(callbacks.poll());
        }
        try {
            long startTime = System.currentTimeMillis();
            syncWal();
            log.trace("Wal Synced in {}ms", System.currentTimeMillis() - startTime);
            for (var callback : callbackCopy) {
                callback.completeAsync(() -> null, callbackExecutor.getExecutor());
            }
        } catch (Throwable t) {
            log.error("Error while syncing wal files", t);
            try {
                log.error("Shutting down fileSystem");
                beansHolder.shutdown();
            } catch (Throwable t2) {
                log.error("Error while closing file system", t2);
            }
        }

    }

    public void syncWal() {
        storage.flushAndSync();
        walDirectory.sync();
    }

    public void requestSync(boolean forceNow) {
        syncRequest.set(true);
        if(forceNow) {
            taskScheduledExecutor.getExecutorService().execute(this::syncWalJob);
        }
    }

    public CompletableFuture<Void> registerCallback() {
        var cf = new CompletableFuture<Void>();
        callbacks.add(cf);
        return cf;
    }
}
