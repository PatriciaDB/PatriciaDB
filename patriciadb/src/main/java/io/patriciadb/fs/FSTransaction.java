package io.patriciadb.fs;

import java.nio.ByteBuffer;

public interface FSTransaction extends BlockWriter, Releasable {

    long write(ByteBuffer data);

    void overwrite(long blockId, ByteBuffer data);

    void delete(long blockId);

    void commit();
}
