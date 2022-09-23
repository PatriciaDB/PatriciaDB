package io.patriciadb.fs.disk.directory;

import io.patriciadb.fs.disk.DirectoryError;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public interface Directory  {

    Roaring64NavigableMap getFreeBlocksMap();

    void expandCapacity() throws DirectoryError;

    long get(long blockId) throws DirectoryError;

    default LongLongHashMap get(LongIterable ids) {
        LongLongHashMap map = new LongLongHashMap();
        ids.forEach(id -> map.put(id, get(id)));
        return map;
    }

    void set(LongLongHashMap changeMap) throws DirectoryError;


}
