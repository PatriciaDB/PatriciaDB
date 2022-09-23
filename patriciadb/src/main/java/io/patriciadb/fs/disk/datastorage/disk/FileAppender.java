package io.patriciadb.fs.disk.datastorage.disk;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface FileAppender extends FileReader {
    void flush() throws IOException;

    void flushAndSync() throws IOException;

    int write(ByteBuffer data) throws FileFullError, IOException;

}
