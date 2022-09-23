package io.patriciadb.fs.disk.datastorage.disk;

import java.io.IOException;

public interface FileDataAppenderFactory {
    FileAppender newFileDataAppender() throws IOException;
}
