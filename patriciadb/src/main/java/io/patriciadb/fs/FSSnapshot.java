package io.patriciadb.fs;

public interface FSSnapshot extends BlockReader, Releasable {

    void release();
}
