package io.patriciadb.fs.disk.datastorage.disk;

import io.patriciadb.utils.VarInt;
import io.patriciadb.fs.disk.StorageIoException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileDataMMapReader implements FileReader {

    private final FileDataChannel channel;

    private volatile ByteBuffer mmapBuffer;

    public FileDataMMapReader(FileDataChannel channel) throws IOException {
        if (channel.size() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid FileData greater than 2GB");
        }
        mmapBuffer = channel.mmap(0, (int) channel.size());
        this.channel = channel;
    }

    @Override
    public FileDataChannel getChannel() {
        return channel;
    }

    public ByteBuffer read(int offset) {
        if (offset >= mmapBuffer.capacity()) {
            throw new StorageIoException("Offset " + offset + " not available");
        }
        int size = VarInt.getVarInt(mmapBuffer, offset);
        int headerLength = VarInt.varIntSize(size);
        return mmapBuffer.slice(headerLength + offset, size);
    }
}
