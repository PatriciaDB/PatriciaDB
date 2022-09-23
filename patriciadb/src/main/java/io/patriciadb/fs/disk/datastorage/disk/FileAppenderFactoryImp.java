package io.patriciadb.fs.disk.datastorage.disk;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FileAppenderFactoryImp implements FileDataAppenderFactory {
    private final Path directory;
    private final int maxAppenderSize;
    private final AtomicInteger nextFileId = new AtomicInteger(1);
    private final AtomicLong sequenceNumber = new AtomicLong();

    public FileAppenderFactoryImp(Path directory, int maxAppenderSize, int nextFileId, long nextSequenceNumber) {
        this.directory = directory;
        this.maxAppenderSize = maxAppenderSize;
        this.nextFileId.set(nextFileId);
        this.sequenceNumber.set(nextSequenceNumber);
    }

    @Override
    public FileAppender newFileDataAppender() throws IOException {
        int fileId = nextFileId.getAndIncrement();
        long sequenceId = sequenceNumber.getAndIncrement();
        String fileName = String.format("%08X", fileId).toLowerCase();
        var channel = FileDataChannel.create(directory.resolve(fileName + ".data"), fileId, sequenceId);
        return new FileDataAppender(channel, FileDataAppender.DEFAULT_BUFFER_SIZE, FileDataAppender.MMAP_DEFAULT_BLOCK_SIZE, maxAppenderSize);
    }
}
