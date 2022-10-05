package io.patriciadb.fs;

public interface FsWriteTransaction extends BlockWriter, Releasable {

    void commit();
}
