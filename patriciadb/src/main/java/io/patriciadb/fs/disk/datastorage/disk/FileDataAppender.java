package io.patriciadb.fs.disk.datastorage.disk;

import io.patriciadb.utils.VarInt;
import io.patriciadb.fs.disk.StorageIoException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Objects;

public class FileDataAppender implements FileAppender {
    public static final long MMAP_DEFAULT_BLOCK_SIZE = 1 << 25;
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private final long mmapExpansionSnapToSize;

    private final long maxFileSize;
    private final FileDataChannel ch;
    private MappedByteBuffer mappedByteBuffer;
    private int mappedBufferSize;
    private int fileWriteCursor;
    private int virtualCursor;
    private final ByteBuffer buffer;

    public FileDataAppender(FileDataChannel ch, int bufferSize) throws IOException {
        this(ch, bufferSize, MMAP_DEFAULT_BLOCK_SIZE, Integer.MAX_VALUE);
    }

    public FileDataAppender(FileDataChannel ch, int bufferSize, long mmapExpansionSnapToSize, long maxFileSize) throws IOException {
        this.ch = Objects.requireNonNull(ch);
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Invalid buffer size. Provided: " + bufferSize);
        }
        if (mmapExpansionSnapToSize <= 0) {
            throw new IllegalArgumentException("Invalid mmapExpansionSnapToSize. Provided:" + mmapExpansionSnapToSize);
        }
        if (maxFileSize < 20 * 1024 * 1024) {
            throw new IllegalArgumentException("MaxFileSize cannot be less than 20MB");
        }
        this.mmapExpansionSnapToSize = mmapExpansionSnapToSize;
        fileWriteCursor = (int) ch.size();
        virtualCursor = fileWriteCursor;
        buffer = ByteBuffer.allocateDirect(bufferSize);
        this.maxFileSize = maxFileSize;
        remap();
    }

    @Override
    public FileDataChannel getChannel() {
        return ch;
    }

    private void writePendingBuffer() throws IOException {
        if (buffer.position() == 0) {
            return;
        }
        buffer.flip();
        fileWriteCursor += ch.write(buffer, fileWriteCursor);
        buffer.clear();
    }

    public synchronized void remap() throws IOException {
        long fileSize = ch.size();
        long mmapSize = fileSize;
        if (mmapSize % mmapExpansionSnapToSize > 0) {
            mmapSize = ((int) (mmapSize / mmapExpansionSnapToSize + 1)) * mmapExpansionSnapToSize;
        }
        if (mmapSize > maxFileSize) {
            mmapSize = maxFileSize;
        }
        if (mmapSize == mappedBufferSize) {
            return;
        }
        mappedByteBuffer = ch.mmap(0, (int) mmapSize);
        mappedBufferSize = (int) mmapSize;

        // We can truncate in linux/macos and still have bigger memoryMap (out of file space)
        // However in Windows the following will not have effect
        ch.truncate(fileSize);
    }


    public synchronized int write(ByteBuffer payload) throws IOException, FileFullError {
        var header = VarInt.varInt(payload.remaining());
        int size = payload.remaining() + header.length;
        if (virtualCursor + (long) size > maxFileSize) {
            throw new FileFullError();
        }
        int position = virtualCursor;
        virtualCursor += size;
        if (buffer.remaining() < size) {
            writePendingBuffer();
            if (buffer.remaining() < size) {
                fileWriteCursor += ch.write(ByteBuffer.wrap(header), fileWriteCursor);
                fileWriteCursor += ch.write(payload, fileWriteCursor);
                return position;
            }
        }
        buffer.put(header);
        buffer.put(payload);
        return position;
    }

    public synchronized void flush() throws IOException {
        writePendingBuffer();
    }

    public synchronized void flushAndSync() throws IOException {
        writePendingBuffer();
        ch.force(false);
    }

    public synchronized ByteBuffer read(int position) throws IOException {
        if (position >= virtualCursor) {
            throw new StorageIoException("Invalid position");
        }
        if (position > fileWriteCursor - 1) { // Read from pending buffer
            position -= fileWriteCursor;
            if (position > buffer.position()) {
                throw new StorageIoException("Invalid position in buffer of " + position + " buffer position is " + buffer.position());
            }
            int size = VarInt.getVarInt(buffer, position);
            int headerLength = VarInt.varIntSize(size);
            byte[] data = new byte[size];
            buffer.get(position + headerLength, data);
            return ByteBuffer.wrap(data);
        }
        if (position >= mappedBufferSize - VarInt.MAX_VARINT_SIZE && mappedBufferSize < ch.size()) { // remap MMAP
            remap();
        }

        if (position >= mappedBufferSize) {
            throw new StorageIoException("Invalid position");
        }
        try {
            int size = VarInt.getVarInt(mappedByteBuffer, position);
            int headerSize = VarInt.varIntSize(size);
            if (size + position + headerSize > mappedBufferSize) {
                remap();
            }
            return mappedByteBuffer.slice(position + headerSize, size);
        } catch (IndexOutOfBoundsException e) {
            throw new StorageIoException("Invalid position " + position, e);
        }
    }
}
