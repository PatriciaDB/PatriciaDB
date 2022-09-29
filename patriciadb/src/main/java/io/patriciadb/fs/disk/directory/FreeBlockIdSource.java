package io.patriciadb.fs.disk.directory;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public interface FreeBlockIdSource {

    Roaring64NavigableMap getFreeBlockId();

    void expandCapacity();
}
