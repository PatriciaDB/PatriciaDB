package io.patriciadb.fs.disk;

import io.patriciadb.fs.FSSnapshot;
import io.patriciadb.fs.FSTransaction;
import io.patriciadb.fs.FileSystemError;
import io.patriciadb.fs.PatriciaFileSystem;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.datastorage.disk.AppenderDataStorage;
import io.patriciadb.fs.disk.directory.*;
import io.patriciadb.fs.disk.directory.transaction.TransactionalDirectoryImp;
import io.patriciadb.fs.disk.directory.versioned.MvccDirectory;
import io.patriciadb.fs.disk.directory.wal.DirectoryLogFileReader;
import io.patriciadb.fs.disk.directory.wal.WalDirectory;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DiskFileSystem implements PatriciaFileSystem {
    private final static Logger log = LoggerFactory.getLogger(DiskFileSystem.class);
    private final AppenderDataStorage dataStorage;
    private final DiskDirectory diskMMapDirectory;
    private final WalDirectory walDirectory;
    private final VersionedDirectory versionedDirectory;
    private final TransactionalDirectory transactionalDirectory;
    private final ScheduledThreadPoolExecutor executorService;
    private final ThreadPoolExecutor callbackExecutor;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final WalSyncController walSyncController;

    public DiskFileSystem(Path dir) throws IOException {
        this(dir, Integer.MAX_VALUE);

    }
    public DiskFileSystem(Path dir, int maxAppenderSize) throws IOException {
        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException(dir + " is not a directory");
        }

        diskMMapDirectory = new DiskMMapDirectory(dir.resolve("directory"));

        // Restore directory transactions from the log file to the directory file
        var walDirFile = dir.resolve("directory.log");
        if (Files.exists(walDirFile)) {
            try (var logReader = new DirectoryLogFileReader(walDirFile)) {
                Optional<LongLongHashMap> opt = Optional.empty();
                while ((opt = logReader.readNextBlock()).isPresent()) {
                    var map = opt.get();
                    log.info("Restoring a transaction with {} changes", map.size());
                    diskMMapDirectory.set(map);
                }
            } catch (Throwable t) {
                log.warn("Wal file reader found a corrupted data, a transaction will not be persisted", t);
            }
            diskMMapDirectory.sync();
        }
        walDirectory = new WalDirectory(diskMMapDirectory, walDirFile, WalDirectory.MAX_WAL_LOG_FILE_SIZE);
        versionedDirectory = new MvccDirectory(walDirectory);
        transactionalDirectory = new TransactionalDirectoryImp(versionedDirectory);
        dataStorage = DataStorageFactory.openDirectory(dir, maxAppenderSize);
        executorService = new ScheduledThreadPoolExecutor(1, new FsThreadFactory());
        callbackExecutor = new ThreadPoolExecutor(1,1,1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        walSyncController = new WalSyncController(walDirectory, dataStorage, this::closeExceptionally,callbackExecutor );
        executorService.scheduleWithFixedDelay(walSyncController::syncWalJob, 100, 200, TimeUnit.MILLISECONDS);

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
    public synchronized void close() {
        checkState();
        try {
            executorService.shutdownNow();
            callbackExecutor.shutdownNow();
            walSyncController.syncWal();
            dataStorage.close();
            walDirectory.close();
            diskMMapDirectory.close();

        } catch (Throwable t) {
            throw new FileSystemError(true, "Failed closing the underline components", t);
        } finally {
            isOpen.set(false);
        }
    }

    private synchronized void closeExceptionally(Throwable t) {
        if (!isOpen.get()) return;
        log.error("FileSystem terminated exceptionally", t);
        close();
    }

    @Override
    public CompletableFuture<Void> syncNow() {
        checkState();
        var callback = walSyncController.registerCallback();
        walSyncController.setSyncRequest();
        executorService.execute(walSyncController::setSyncRequest);
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
            walSyncController.setSyncRequest();
        }

        @Override
        public void release() {
            transaction.release();
        }
    }
}
