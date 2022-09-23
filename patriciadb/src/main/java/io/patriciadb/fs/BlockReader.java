package io.patriciadb.fs;

import java.nio.ByteBuffer;

public interface BlockReader {
    ByteBuffer read(long blockId);
}
