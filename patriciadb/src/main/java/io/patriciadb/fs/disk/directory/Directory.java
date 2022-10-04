package io.patriciadb.fs.disk.directory;

import io.patriciadb.fs.disk.DirectoryError;
import io.patriciadb.fs.disk.utils.LongLongPair;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.List;
import java.util.function.Predicate;

public interface Directory  {

    long get(long blockId) throws DirectoryError;

    void forEach(BlockIdConsumer consumer);

    default LongLongHashMap get(LongIterable ids) {
        LongLongHashMap map = new LongLongHashMap();
        ids.forEach(id -> map.put(id, get(id)));
        return map;
    }

    void set(LongLongHashMap changeMap) throws DirectoryError;

    interface BlockIdConsumer {
        void consume(long blockId, long pointer);
    }

}
