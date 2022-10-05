package io.patriciadb.fs.disk.transaction;

import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.directory.Directory;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TransactionSessionAbs {
    protected final TransactionHandler transactionHandler;
    protected final AtomicReference<TransactionStatus> status = new AtomicReference<>(TransactionStatus.RUNNING);
    protected final long transactionId;
    protected final List<LongLongHashMap> prevChanges = new ArrayList<>();
    protected final Instant creationTime = Instant.now();
    protected final DataStorage dataStorage;
    protected final Directory directory;

    public TransactionSessionAbs(TransactionHandler transactionHandler,
                                 long transactionId,
                                 DataStorage dataStorage,
                                 Directory directory) {
        this.transactionHandler = transactionHandler;
        this.transactionId = transactionId;
        this.dataStorage = dataStorage;
        this.directory = directory;
    }


    public TransactionStatus getStatus() {
        return status.get();
    }

    public void setStatus(TransactionStatus newStatus) {
        status.set(newStatus);
    }

    public synchronized void addDeltaChange(LongLongHashMap deltaChange) {
        prevChanges.add(deltaChange);
    }

    public long getId() {
        return transactionId;
    }
}
