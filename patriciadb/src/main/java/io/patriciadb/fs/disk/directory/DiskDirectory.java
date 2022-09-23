package io.patriciadb.fs.disk.directory;

import io.patriciadb.fs.disk.DirectoryError;

import java.io.Closeable;

public interface DiskDirectory extends Directory, Closeable {

    void sync() throws DirectoryError;

}
