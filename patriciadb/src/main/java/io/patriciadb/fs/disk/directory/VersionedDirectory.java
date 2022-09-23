package io.patriciadb.fs.disk.directory;

import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

public interface VersionedDirectory extends Directory {

    void deleteOldSnapshots(long version);

    long get(long version, long blockId);


    long setAndGetVersion(LongLongHashMap batch);

    default void set(LongLongHashMap batch) {
        setAndGetVersion(batch);
    }

    long getCurrentVersion();
}
