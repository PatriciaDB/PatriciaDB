package io.patriciadb.fs.disk.directory;

import io.patriciadb.fs.disk.DirectoryError;

import java.io.Closeable;

public interface DiskDirectory extends Directory {

    void sync() throws DirectoryError;

}
