package io.patriciadb.fs.disk.directory.imp;

import io.patriciadb.fs.disk.DirectoryError;
import io.patriciadb.fs.disk.directory.Directory;
import io.patriciadb.fs.disk.directory.DiskDirectory;
import io.patriciadb.utils.Space;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class WriteAheadLogDirectory implements DiskDirectory, PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(WriteAheadLogDirectory.class);
    public static final long DEFAULT_MAX_WAL_LOG_FILE_SIZE = Space.MEGABYTE.toBytes(100);
    private final Directory directory;
    private final LongLongHashMap mergedMaps = new LongLongHashMap();
    private final LogFileWriter logWriter;
    private final long maxLogWriterSize;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Path dirLogFile;

    public WriteAheadLogDirectory(Directory directory, Path dirLogFile, long maxLogWriteSize) throws IOException {
        this.directory = directory;
        this.dirLogFile = dirLogFile;
        this.logWriter = new LogFileWriter(dirLogFile);
        this.maxLogWriterSize = maxLogWriteSize;
    }

    @Override
    public void initialize() throws Exception {
        log.info("Restoring old transactions from log file");
        if (Files.exists(dirLogFile)) {
            try (var logReader = new LogFileReader(dirLogFile)) {
                Optional<LongLongHashMap> opt = Optional.empty();
                while ((opt = logReader.readNextBlock()).isPresent()) {
                    var map = opt.get();
                    log.info("Restoring a transaction with {} changes", map.size());
                    directory.set(map);
                }
            } catch (Throwable t) {
                log.warn("Wal file reader found a corrupted data, a transaction will not be persisted", t);
            }
            if (directory instanceof DiskDirectory diskDirectory) {
                diskDirectory.sync();
            }
        }
        logWriter.reset();
    }

    @Override
    public void forEach(BlockIdConsumer consumer) {
        checkState();
        if (mergedMaps.isEmpty()) {
            directory.forEach(consumer);
        } else {
            directory.forEach(((blockId, pointer) -> {
                if (mergedMaps.containsKey(blockId)) {
                    consumer.consume(blockId, mergedMaps.get(blockId));
                } else {
                    consumer.consume(blockId, pointer);
                }
            }));
        }
    }

    private void checkState() {
        if (!isOpen.get()) {
            throw new DirectoryError(true, "Directory is closed");
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
        writeToLogFileAndSync(false);

    }

    @Override
    public void destroy() throws Exception {
        log.info("Closing LogDirectory");
        try {
            writeToLogFileAndSync(true);
            logWriter.close();
        } catch (IOException e) {
            throw new DirectoryError(true, e);
        } finally {
            isOpen.set(false);
        }
    }

    private synchronized void writeToLogFileAndSync(boolean forceSyncChildDir) throws DirectoryError {
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
