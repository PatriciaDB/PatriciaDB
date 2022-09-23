package io.patriciadb.fs.disk.directory.wal;

import io.patriciadb.fs.disk.directory.Directory;
import io.patriciadb.fs.disk.DirectoryError;
import io.patriciadb.fs.disk.directory.DiskDirectory;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class WalDirectory implements DiskDirectory {
    private final static Logger log = LoggerFactory.getLogger(WalDirectory.class);
    public static final long MAX_WAL_LOG_FILE_SIZE = 50 * 1024 * 1024; //50MB
    private final Directory directory;
    private final LongLongHashMap mergedMaps = new LongLongHashMap();
    private final DirectoryLogFileWriter logWriter;
    private final long maxLogWriterSize;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    public WalDirectory(Directory directory, Path dirLogFIle, long maxLogWriteSize) throws IOException {
        this.directory = directory;
        this.logWriter = new DirectoryLogFileWriter(dirLogFIle);
        this.maxLogWriterSize = maxLogWriteSize;
    }

    private void checkState() {
        if (!isOpen.get()) {
            throw new DirectoryError(true, "Directory is closed");
        }
    }

    public synchronized Roaring64NavigableMap getFreeBlocksMap() {
        checkState();
        var bitmap = directory.getFreeBlocksMap();
        for (var kv : mergedMaps.keyValuesView()) {
            long k = kv.getOne();
            long v = kv.getTwo();
            if (v == 0) {
                bitmap.addLong(k);
            } else {
                bitmap.removeLong(k);
            }
        }
        return bitmap;
    }

    @Override
    public void expandCapacity() throws DirectoryError {
        checkState();
        directory.expandCapacity();
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            sync(true);
            logWriter.close();
        } catch (IOException e) {
            throw new DirectoryError(true, e);
        } finally {
            isOpen.set(false);
        }
    }

    @Override
    public synchronized long get(long id) {
        return mergedMaps.containsKey(id) ? mergedMaps.get(id) : directory.get(id);
    }


    @Override
    public synchronized LongLongHashMap get(LongIterable ids) {
        checkState();
        var res = new LongLongHashMap(ids.size());
        ids.forEach(id -> res.put(id, get(id)));
        return res;
    }

    /**
     * Set the new Map making it available immediately. This method doesn't write to the underling directory.
     *
     * @param batch
     */
    public synchronized void set(LongLongHashMap batch) {
        checkState();
        mergedMaps.putAll(batch);
    }

    /**
     * Writes the pending change to the wal log file and calling Sync on the wal file.
     *
     * @throws DirectoryError when an io error is generated
     */
    public synchronized void sync() throws DirectoryError {
        checkState();
        sync(false);

    }

    private synchronized void sync(boolean forceSyncChildDir) throws DirectoryError {
        checkState();
        try {
            if (!mergedMaps.isEmpty()) {
                logWriter.appendAndSync(mergedMaps);
                directory.set(mergedMaps);
                mergedMaps.clear();
            }
            if (logWriter.getLogSize() > maxLogWriterSize || forceSyncChildDir) {
                log.trace("Sync child directory and cleaning log file");
                if (directory instanceof DiskDirectory diskDirectory) {
                    diskDirectory.sync();
                }
                logWriter.reset();
            }

        } catch (IOException e) {
            throw new DirectoryError(true, e);
        }

    }
}
