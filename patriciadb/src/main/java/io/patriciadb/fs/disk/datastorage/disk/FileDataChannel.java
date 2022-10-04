package io.patriciadb.fs.disk.datastorage.disk;

import io.patriciadb.utils.ExceptionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileDataChannel implements Closeable {
    public enum OpenMode {READ_ONLY,READ_WRITE}

    private final Path path;
    private final FileChannel ch;
    private final FileDataHeader header;
    private final OpenMode openMode;
    private final AtomicBoolean isReadOnly = new AtomicBoolean(false);

    private FileDataChannel(Path path, FileChannel ch, FileDataHeader header, OpenMode openMode) {
        this.path = path;
        this.ch = ch;
        this.header = header;
        this.openMode = openMode;
        isReadOnly.set(openMode == OpenMode.READ_ONLY);
    }

    public int getFileId() {
        return header.fileId();
    }

    public long write(ByteBuffer buffer, long position) throws IOException{
        if(isReadOnly.get()) {
            throw new IllegalStateException("FileData is readonly");
        }
        return ch.write(buffer, position);
    }

    public MappedByteBuffer mmap(long offset, int size) throws IOException {
        return ch.map(FileChannel.MapMode.READ_ONLY, offset, size);
    }

    public void makeReadOnly() {
        isReadOnly.set(true);
    }

    public boolean isReadOnly() {
        return isReadOnly.get();
    }

    public FileDataHeader getHeader() {
        return header;
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    public void closeAndDelete() throws IOException {
        close();
        Files.delete(path);
    }

    public long size() throws IOException {
        return ch.size();
    }

    public void truncate(long to) throws IOException {
        if(isReadOnly.get()) {
            throw new IllegalStateException("FileData is readonly");
        }
        ch.truncate(to);
    }

    public void force(boolean metadata) throws IOException{
        ch.force(metadata);
    }
    public static FileDataChannel create(Path path, int fileId, long sequenceNumber) throws IOException{
        if(Files.exists(path)) {
            throw new IOException("FileData does exist: "+path);
        }
        Set<StandardOpenOption> openOptionSet = new HashSet<>();
        openOptionSet.add(StandardOpenOption.READ);
        openOptionSet.add(StandardOpenOption.CREATE_NEW);
        openOptionSet.add(StandardOpenOption.WRITE);

        FileChannel ch = FileChannel.open(path,openOptionSet);
        try {
            var header = FileDataHeader.writeHeader(ch, fileId, sequenceNumber);
            return new FileDataChannel(path, ch, header, OpenMode.READ_WRITE);
        }catch (Throwable e) {
            ch.close();
            throw ExceptionUtils.sneakyThrow(e);
        }
    }

    public static FileDataChannel open(Path path, OpenMode mode) throws IOException {
        if(!Files.exists(path)) {
            throw new IOException("FileData doesn't exist: "+path);
        }
        Set<StandardOpenOption> openOptionSet = new HashSet<>();
        openOptionSet.add(StandardOpenOption.READ);
        if(mode==OpenMode.READ_WRITE) {
            openOptionSet.add(StandardOpenOption.WRITE);
        }
        FileChannel ch = FileChannel.open(path,openOptionSet);
        try {
            var header = FileDataHeader.readHeader(ch);
            return new FileDataChannel(path, ch, header, mode);
        }catch (Throwable e) {
            ch.close();
            throw ExceptionUtils.sneakyThrow(e);
        }
    }
}
