package io.patriciadb.fs.disk.directory.transaction;

import io.patriciadb.fs.disk.*;
import io.patriciadb.fs.disk.directory.*;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionalDirectoryImp implements TransactionalDirectory {
    private final static Logger log = LoggerFactory.getLogger(TransactionalDirectoryImp.class);

    private final VersionedDirectory versionedDirectory;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final HashSet<DirectorySnapshot> openTransactions = new HashSet<>();

    public TransactionalDirectoryImp(VersionedDirectory versionedDirectory) {
        this.versionedDirectory = versionedDirectory;
    }


    public synchronized DirectorySnapshot getSnapshot() {
        checkState();
        var tr= new LocalDirectorySnapshot(versionedDirectory.getCurrentVersion());
        openTransactions.add(tr);
        return tr;
    }

    public synchronized LocalDirTransaction starTransaction() {
        checkState();
        var tr= new LocalDirTransaction(versionedDirectory.getCurrentVersion());
        openTransactions.add(tr);
        return tr;
    }

    private synchronized void release(DirectorySnapshot transation) {
        if(openTransactions.remove(transation)) {
            long maxVersionId = openTransactions.stream()
                    .mapToLong(DirectorySnapshot::getVersion)
                    .max()
                    .orElse(versionedDirectory.getCurrentVersion());
            versionedDirectory.deleteOldSnapshots(maxVersionId);
        }

    }

    private void checkState() {
        if(!isOpen.get()) {
            throw new DirectoryError(true, "Directory is closed");
        }
    }

    @Override
    public void close() {
        isOpen.set(false);
    }

    private synchronized void commit(LocalDirTransaction transaction) {
        checkState();
        if (versionedDirectory.getCurrentVersion() != transaction.transactionVersion) {
            throw new OptimisticLockingFailure("Invalid transaction version, expected " + versionedDirectory.getCurrentVersion() + " found " + transaction.transactionVersion);
        }
        versionedDirectory.setAndGetVersion(transaction.pending);
    }

    private class LocalDirectorySnapshot implements DirectorySnapshot {

        private final long directoryVersion;

        public LocalDirectorySnapshot(long directoryVersion) {
            this.directoryVersion = directoryVersion;
        }

        @Override
        public long get(long blockid) {
            return versionedDirectory.get(directoryVersion, blockid);
        }

        @Override
        public long getVersion() {
            return directoryVersion;
        }

        @Override
        public void release() {
            TransactionalDirectoryImp.this.release(this);
        }

    }

    private class LocalDirTransaction implements DirectoryTransaction {
        private final FreeBlockIdGenerator idGenerator;
        private final long transactionVersion;
        private final LongLongHashMap pending = new LongLongHashMap();

        public LocalDirTransaction(long transactionVersion) {
            this.idGenerator = new FreeBlockIdGenerator(versionedDirectory);
            this.transactionVersion = transactionVersion;
        }

        @Override
        public long get(long blockid) {
            if (pending.containsKey(blockid)) {
                return pending.get(blockid);
            }
            return versionedDirectory.get(transactionVersion, blockid);
        }

        @Override
        public long getVersion() {
            return transactionVersion;
        }

        @Override
        public void set(long blockId, long position) {
            pending.put(blockId, position);
        }

        @Override
        public void clear(long blockId) {
            pending.put(blockId, 0L);
        }

        @Override
        public long getNextFreeBlockId() {
            return idGenerator.nextBlockId();
        }

        @Override
        public void commit() throws OptimisticLockingFailure {
            TransactionalDirectoryImp.this.commit(this);
            TransactionalDirectoryImp.this.release(this);
        }

        @Override
        public void release() {
            TransactionalDirectoryImp.this.release(this);
        }
    }
}
