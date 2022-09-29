package io.patriciadb.fs.disk;

import io.patriciadb.fs.FSSnapshot;
import io.patriciadb.fs.FSTransaction;
import io.patriciadb.fs.FileSystemError;
import io.patriciadb.fs.PatriciaFileSystem;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.datastorage.disk.AppenderDataStorage;
import io.patriciadb.fs.disk.directory.*;
import io.patriciadb.utils.lifecycle.BeansHolder;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DiskFileSystem implements PatriciaFileSystem, PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(DiskFileSystem.class);
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final BeansHolder beansHolder;
    private final AppenderDataStorage dataStorage;
    private final TransactionalDirectory transactionalDirectory;
    private final WalSyncController walSyncController;

    public DiskFileSystem(BeansHolder beansHolder,
                          AppenderDataStorage dataStorage,
                          TransactionalDirectory transactionalDirectory,
                          WalSyncController walSyncController) {
        this.beansHolder = beansHolder;
        this.dataStorage = dataStorage;
        this.transactionalDirectory = transactionalDirectory;
        this.walSyncController = walSyncController;
    }

    @Override
    public FSSnapshot getSnapshot() {
        return new LocalReadTransaction(transactionalDirectory.getSnapshot(), dataStorage);
    }

    @Override
    public FSTransaction startTransaction() {
        return new LocalTransaction(transactionalDirectory.starTransaction());
    }

    @Override
    public synchronized void close() throws FileSystemError{
        if(!isOpen.compareAndSet(true, false)) {
            return;
        }
        try {
            beansHolder.shutdown();
        } catch (FileSystemError e) {
            throw e;
        } catch (Throwable t) {
            throw new FileSystemError(true, t);
        } finally {
            isOpen.set(false);
        }
    }



    @Override
    public CompletableFuture<Void> syncNow() {
        checkState();
        var callback = walSyncController.registerCallback();
        walSyncController.requestSync(true);
        return callback;
    }

    private void checkState() {
        if (!isOpen.get()) {
            throw new DirectoryError(true, "Directory is closed");
        }
    }

    private static class LocalReadTransaction implements FSSnapshot {
        private final DirectorySnapshot directorySnapshot;
        private final DataStorage dataStorage;

        public LocalReadTransaction(DirectorySnapshot directorySnapshot, DataStorage dataStorage) {
            this.directorySnapshot = directorySnapshot;
            this.dataStorage = dataStorage;
        }

        @Override
        public ByteBuffer read(long blockId) {

            long pointer = directorySnapshot.get(blockId);
            if (pointer == 0) {
                return null;
            }
            return dataStorage.read(pointer);
        }

        @Override
        public void release() {
            directorySnapshot.release();
        }
    }

    private class LocalTransaction implements FSTransaction {
        private final DirectoryTransaction transaction;

        public LocalTransaction(DirectoryTransaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public ByteBuffer read(long blockId) {
            var pointer = transaction.get(blockId);
            if (pointer == 0) {
                return null;
            }
            var buffer= dataStorage.read(pointer);
            return buffer;
        }

        @Override
        public long write(ByteBuffer buffer) {
            long id = transaction.getNextFreeBlockId();
            overwrite(id, buffer);
            return id;
        }

        @Override
        public void overwrite(long blockId, ByteBuffer buffer) {
            long filePointer = dataStorage.write(buffer);
            transaction.set(blockId, filePointer);
        }

        @Override
        public void delete(long blockId) {
            transaction.clear(blockId);
        }


        public void commit() {
            transaction.commit();
            transaction.release();
            walSyncController.requestSync(true);
        }

        @Override
        public void release() {
            transaction.release();
        }
    }
}
