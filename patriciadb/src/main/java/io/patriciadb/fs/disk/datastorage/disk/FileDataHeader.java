package io.patriciadb.fs.disk.datastorage.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;

public record FileDataHeader(int version, int fileId, long sequenceNumber, Instant createdTime) {
    public static final int DATA_HEADER_MAGIC_NUMBER = 0XCAFECAFE;
    public static final int DATA_HEADER_VERSION = 1;

    public static FileDataHeader writeHeader(FileChannel ch, int fileId, long sequenceNumber) throws IOException {
        var creationTime = Instant.now();
        var header = ByteBuffer.allocate(1024);
        header.putInt(DATA_HEADER_MAGIC_NUMBER);
        header.putInt(DATA_HEADER_VERSION);
        header.putInt(fileId);
        header.putLong(creationTime.toEpochMilli());
        header.putLong(sequenceNumber);
        header.position(0);
        ch.write(header, 0);
        ch.force(true);
        return new FileDataHeader(DATA_HEADER_VERSION, fileId, sequenceNumber, creationTime);
    }


    public static FileDataHeader readHeader(FileChannel ch) throws IOException, FileDataHeaderInvalidException {
        if (ch.size() < 1024) {
            throw new FileDataHeaderInvalidException("Size of file smaller than 1024");
        }
        var buffer = ByteBuffer.allocate(1024);
        ch.read(buffer, 0);
        buffer.flip();
        int headerMagicNumber = buffer.getInt();
        if (headerMagicNumber != DATA_HEADER_MAGIC_NUMBER) {
            throw new FileDataHeaderInvalidException("Invalid Header format");
        }
        int version = buffer.getInt();
        if (version != DATA_HEADER_VERSION) {
            throw new FileDataHeaderInvalidException("Invalid Header version");
        }
        int fileId = buffer.getInt();
        var createTime = Instant.ofEpochMilli(buffer.getLong());
        long sequenceNumber = buffer.getLong();
        return new FileDataHeader(version, fileId, sequenceNumber, createTime);
    }

}
