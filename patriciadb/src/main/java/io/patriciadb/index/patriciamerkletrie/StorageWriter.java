package io.patriciadb.index.patriciamerkletrie;

import java.nio.ByteBuffer;

public interface StorageWriter {

    long write(ByteBuffer byteBuffer);

    void write(long nodeId, ByteBuffer buffer);

    long getFreeBlockId();
}
