package io.patriciadb.fs.disk.datastorage.disk;

import io.patriciadb.fs.FileSystemError;
import io.patriciadb.fs.disk.StorageIoException;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.utils.DiskUtils;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class AppenderDataStorage implements DataStorage, PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(AppenderDataStorage.class);
    private final ConcurrentHashMap<Integer, FileReader> readers = new ConcurrentHashMap<>();
    private volatile FileAppender currentAppender;
    private final FileDataAppenderFactory fileAppenderFactory;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    public AppenderDataStorage(List<FileReader> readers,
                               FileAppender currentAppender,
                               FileDataAppenderFactory fileAppenderFactory) {

        for (var reader : readers) {
            this.readers.put(reader.getChannel().getFileId(), reader);
        }
        this.currentAppender = Objects.requireNonNull(currentAppender);
        if(!this.readers.contains(currentAppender.getChannel().getFileId())) {
            this.readers.put(currentAppender.getChannel().getFileId(), currentAppender);
        }
        this.fileAppenderFactory = Objects.requireNonNull(fileAppenderFactory);
    }

    public synchronized ByteBuffer read(long blockPointer)  {
        checkState();
        try {
            int fileId = DiskUtils.fileId(blockPointer);
            int offset = DiskUtils.offset(blockPointer);
            var fileReader = readers.get(fileId);
            if (fileReader == null) {
                throw new StorageIoException("FileId " + fileId + " not available");
            }
            return fileReader.read(offset);
        }catch (IOException ex) {
            throw new StorageIoException(ex);
        }
    }

    public synchronized void flush() throws StorageIoException {
        checkState();
        try {
            currentAppender.flush();
        }catch (IOException e) {
            throw new StorageIoException(e);
        }
    }

    public synchronized void flushAndSync() {
        checkState();
        try {
            currentAppender.flushAndSync();
        }catch (IOException e) {
            throw new StorageIoException(e);
        }
    }

    private synchronized FileAppender rollAppenderInternal() throws IOException {
        currentAppender.flushAndSync();
        var newAppender = fileAppenderFactory.newFileDataAppender();
        readers.put(newAppender.getChannel().getFileId(), newAppender);
        currentAppender = newAppender;
        return newAppender;
    }

    public synchronized long write(ByteBuffer payload)  {
        checkState();
        try {
            int fileId = currentAppender.getChannel().getFileId();
            int position = currentAppender.write(payload);
            return DiskUtils.combine(fileId, position);
        } catch (FileFullError ex) {
            try {
                var newAppender= rollAppenderInternal();
                int fileId = newAppender.getChannel().getFileId();
                int position = newAppender.write(payload);
                return DiskUtils.combine(fileId, position);
            } catch (FileFullError ex2) {
                throw new StorageIoException("Newly FileAppender is already full for the first element", ex2);
            } catch (IOException e) {
                throw new StorageIoException(e);
            }
        }catch (IOException e) {
            throw new StorageIoException(e);
        }
    }

    private void checkState() {
        if(!isOpen.get()) {
            throw new FileSystemError(true, "Storage is closed");
        }
    }

    public synchronized void close() {
        isOpen.set(false);
        for(var reader: readers.values()) {
            try {
                reader.getChannel().close();
            } catch (Throwable t) {
                log.error("Error when closing storage file", t);
            }
        }
    }

}
