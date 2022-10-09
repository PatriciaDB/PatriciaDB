package io.patriciadb.fs.disk.directory;

import io.patriciadb.fs.disk.DirectoryError;
import io.patriciadb.fs.disk.transaction.Batch;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public interface Directory  {

    long get(long blockId) throws DirectoryError;

    void forEach(BlockIdConsumer consumer);

    default LongLongHashMap get(LongIterable ids) {
        LongLongHashMap map = new LongLongHashMap();
        ids.forEach(id -> map.put(id, get(id)));
        return map;
    }

    default void forEach(Roaring64NavigableMap bitmap, BlockIdConsumer consumer) {
        bitmap.forEach(blockId -> {
            consumer.consume(blockId, get(blockId));
        });
    }

    void set(Batch changeMap) throws DirectoryError;

    void set(LongLongHashMap batch) throws DirectoryError;

    interface BlockIdConsumer {
        void consume(long blockId, long pointer);
    }

}
