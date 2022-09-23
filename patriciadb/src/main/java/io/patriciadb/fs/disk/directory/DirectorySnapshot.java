package io.patriciadb.fs.disk.directory;

public interface DirectorySnapshot {

    long get(long blockid);

    long getVersion();

    void release();
}
