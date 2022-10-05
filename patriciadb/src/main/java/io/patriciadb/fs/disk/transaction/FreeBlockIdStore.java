package io.patriciadb.fs.disk.transaction;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public interface FreeBlockIdStore {

    long getNextFreeBlockId();

    void addFreeBlockIds(Roaring64NavigableMap freeBlocks);


}
