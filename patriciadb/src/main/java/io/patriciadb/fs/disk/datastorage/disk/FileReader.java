package io.patriciadb.fs.disk.datastorage.disk;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface FileReader {


    ByteBuffer read(int offset) throws IOException;

    FileDataChannel getChannel();

}
