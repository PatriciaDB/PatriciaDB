package io.patriciadb.fs.disk.datastorage.disk;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class FileAppenderFactoryImp implements FileDataAppenderFactory {
    private final Path directory;
    private final int maxAppenderSize;
    public  final AtomicInteger nextFileId = new AtomicInteger(1);

    public FileAppenderFactoryImp(Path directory,int maxAppenderSize, int nextFileId) {
        this.directory = directory;
        this.maxAppenderSize = maxAppenderSize;
        this.nextFileId.set(nextFileId);
    }

    @Override
    public FileAppender newFileDataAppender() throws IOException {
        int fileId = nextFileId.getAndIncrement();
        var channel = FileDataChannel.create(directory.resolve(fileId+".data"), fileId);
        return new FileDataAppender(channel,FileDataAppender.DEFAULT_BUFFER_SIZE, FileDataAppender.MMAP_DEFAULT_BLOCK_SIZE, maxAppenderSize);
    }
}
