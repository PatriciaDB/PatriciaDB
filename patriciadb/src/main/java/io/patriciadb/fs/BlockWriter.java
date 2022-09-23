package io.patriciadb.fs;

import java.nio.ByteBuffer;

public interface BlockWriter extends BlockReader{

    long write(ByteBuffer data);

    void overwrite(long blockId, ByteBuffer data);

    void delete(long blockId);
}
