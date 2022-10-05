package io.patriciadb.fs.disk.transaction;

import io.patriciadb.fs.FsWriteTransaction;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.directory.Directory;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.nio.ByteBuffer;

public class TransactionWriteSession extends TransactionSessionAbs implements TransactionSession {
    private final LongLongHashMap changes = new LongLongHashMap();
    private final Roaring64NavigableMap newBlockIds = new Roaring64NavigableMap();
    private final FreeBlockIdStore freeBlockIdStore;

    public TransactionWriteSession(TransactionHandler transactionHandler,
                                   long transactionId,
                                   DataStorage dataStorage,
                                   Directory directory,
                                   FreeBlockIdStore freeBlockIdStore) {
        super(transactionHandler, transactionId, dataStorage, directory);
        this.freeBlockIdStore = freeBlockIdStore;
    }

    public Roaring64NavigableMap getNewBlockIds() {
        return newBlockIds;
    }

    public LongLongHashMap getChanges() {
        return changes;
    }

    public FsWriteTransaction createSession() {
        return new FsWriteTransactionImp();
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
            throw new IllegalStateException("Transaction is disposed");
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
            transactionHandler.commit(TransactionWriteSession.this);
        }

        @Override
        public synchronized void release() {
            transactionHandler.release(TransactionWriteSession.this);
        }
    }

}
