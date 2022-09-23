package io.patriciadb.fs.disk.directory;

public interface TransactionalDirectory  {

    DirectorySnapshot getSnapshot();

    DirectoryTransaction starTransaction();

    void close();

}
