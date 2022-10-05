package io.patriciadb.fs.disk.transaction;

import io.patriciadb.fs.FsReadTransaction;
import io.patriciadb.fs.FsWriteTransaction;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.directory.Directory;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TransactionSession {
    private final TransactionHandler transactionHandler;
    private final AtomicReference<TransactionStatus> status = new AtomicReference<>(TransactionStatus.RUNNING);
    private final long transactionId;
    private final List<LongLongHashMap> prevChanges = new ArrayList<>();
    private final LongLongHashMap changes = new LongLongHashMap();
    private final Roaring64NavigableMap newBlockIds = new Roaring64NavigableMap();
    private final Instant creationTime = Instant.now();
    private final DataStorage dataStorage;
    private final Directory directory;
    private final FreeBlockIdStore freeBlockIdStore;

    public TransactionSession(TransactionHandler transactionHandler,
                              long transactionId,
                              DataStorage dataStorage,
                              Directory directory,
                              FreeBlockIdStore freeBlockIdStore) {
        this.transactionHandler = transactionHandler;
        this.transactionId = transactionId;
        this.dataStorage = dataStorage;
        this.directory = directory;
        this.freeBlockIdStore = freeBlockIdStore;
    }

    public Roaring64NavigableMap getNewBlockIds() {
        return newBlockIds;
    }

    public synchronized void addDeltaChange(LongLongHashMap deltaChange) {
        prevChanges.add(deltaChange);
    }

    public long getId() {
        return transactionId;
    }

    public LongLongHashMap getChanges() {
        return changes;
    }

    public TransactionStatus getStatus() {
        return status.get();
    }

    public void setStatus(TransactionStatus newStatus) {
        status.set(newStatus);
    }

    public FsWriteTransaction createWriteSession() {
        return new FsWriteTransactionImp();
    }

    public FsReadTransaction createReadSession() {
        return new FsReadTransactionImp();
    }

    private synchronized long readPointer(long blockId) {
        for (var elem : prevChanges) {
            if (elem.containsKey(blockId)) {
                return elem.get(blockId);
            }
        }
        return directory.get(blockId);
    }

    private void checkState() {
        if(status.get()!=TransactionStatus.RUNNING) {
            throw new IllegalStateException("Transaction is not disposed");
        }
    }


    private class FsWriteTransactionImp implements FsWriteTransaction {


        @Override
        public synchronized ByteBuffer read(long blockId) {
            checkState();
            long dataPointer = changes.containsKey(blockId)
                    ? changes.get(blockId)
                    : readPointer(blockId);
            if (dataPointer == 0) {
                return null;
            }
            return dataStorage.read(dataPointer);
        }

        @Override
        public synchronized long write(ByteBuffer data) {
            checkState();
            long dataPointer = dataStorage.write(data);
            long blockId = freeBlockIdStore.getNextFreeBlockId();
            newBlockIds.add(blockId);
            changes.put(blockId, dataPointer);
            return blockId;
        }

        @Override
        public synchronized void overwrite(long blockId, ByteBuffer data) {
            checkState();
            long dataPointer = dataStorage.write(data);
            changes.put(blockId, dataPointer);
        }

        @Override
        public synchronized void delete(long blockId) {
            checkState();
            changes.put(blockId, 0);
        }

        @Override
        public synchronized void commit() {
            checkState();
            transactionHandler.commit(TransactionSession.this);
        }

        @Override
        public synchronized void release() {
            transactionHandler.release(TransactionSession.this);
        }
    }

    private class FsReadTransactionImp implements FsReadTransaction {

        @Override
        public synchronized ByteBuffer read(long blockId) {
            checkState();
            long dataPointer = changes.containsKey(blockId)
                    ? changes.get(blockId)
                    : readPointer(blockId);
            if (dataPointer == 0) {
                return null;
            }
            return dataStorage.read(dataPointer);
        }

        @Override
        public synchronized void release() {
            transactionHandler.release(TransactionSession.this);
        }
    }
}
