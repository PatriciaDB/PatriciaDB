package io.patriciadb.fs.disk.directory.imp;

import io.patriciadb.fs.disk.DirectoryError;
import io.patriciadb.fs.disk.directory.Directory;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class InMemoryDirectory implements Directory, Closeable, PatriciaController {

    private final LongLongHashMap directory = new LongLongHashMap();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    private void checkState() {
        if(!isOpen.get()) {
            throw new DirectoryError(true, "Directory is closed");
        }
    }

    @Override
    public void forEach(BlockIdConsumer consumer) {
        directory.forEachKeyValue(consumer::consume);
    }



    @Override
    public synchronized long get(long blockId) throws DirectoryError {
        checkState();
        return directory.getIfAbsent(blockId, 0);
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen.set(false);
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
            } else {
                directory.put(k, v);
            }
        }
    }
}
