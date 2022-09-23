package io.patriciadb;

public interface Releasable {
    /**
     * Release the current transaction/snapshot and free the memory
     */
    void release();
}
