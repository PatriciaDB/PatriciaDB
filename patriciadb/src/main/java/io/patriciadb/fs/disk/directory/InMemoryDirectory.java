package io.patriciadb.fs.disk.directory;

import io.patriciadb.fs.disk.DirectoryError;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class InMemoryDirectory implements Directory, Closeable {

    private final LongLongHashMap directory = new LongLongHashMap();
    private final Roaring64NavigableMap freeBlockIdMap = new Roaring64NavigableMap();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    public InMemoryDirectory() {
        freeBlockIdMap.add(1, 300_000);
    }

    @Override
    public synchronized Roaring64NavigableMap getFreeBlocksMap() {
        checkState();
        var copy = new Roaring64NavigableMap(false);
        copy.or(freeBlockIdMap);
        return copy;
    }

    private void checkState() {
        if(!isOpen.get()) {
            throw new DirectoryError(true, "Directory is closed");
        }
    }

    @Override
    public synchronized void expandCapacity() throws DirectoryError {
        checkState();
        long maxBlockId = directory.keysView().maxIfEmpty(1);
        freeBlockIdMap.add(maxBlockId + 1, maxBlockId + 100_000);
    }

    @Override
    public synchronized long get(long blockId) throws DirectoryError {
        checkState();
        return directory.getIfAbsent(blockId, 0);
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen.set(false);
        freeBlockIdMap.clear();
        directory.clear();
    }

    @Override
    public synchronized void set(LongLongHashMap changeMap) throws DirectoryError {
        checkState();
        for (var e : changeMap.keyValuesView()) {
            long k = e.getOne();
            long v = e.getTwo();
            if (v == 0) {
                directory.remove(k);
                freeBlockIdMap.addLong(k);
            } else {
                directory.put(k, v);
                freeBlockIdMap.removeLong(k);

            }
        }
    }
}
