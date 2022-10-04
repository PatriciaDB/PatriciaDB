package io.patriciadb.fs.disk.directory;

public interface BlockIdPredicate {
    boolean test(long blockId, long pointer);
}
