package io.patriciadb.fs.disk.datastorage.inmemory;

import io.patriciadb.fs.disk.datastorage.DataStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryStorage implements DataStorage {
    private final ConcurrentHashMap<Long, ByteBuffer> storageMap = new ConcurrentHashMap<>();
    private final AtomicLong generator = new AtomicLong(1);
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    @Override
    public ByteBuffer read(long blockPointer)  {
        return storageMap.get(blockPointer);
    }

    @Override
    public void flush()  {
        //Nope
    }

    @Override
    public void flushAndSync()  {
        //Nope
    }

    @Override
    public long write(ByteBuffer payload)  {
        ByteBuffer copy = ByteBuffer.allocate(payload.remaining());
        copy.put(payload);
        copy.flip();
        storageMap.put(generator.getAndIncrement(), copy);
        return 0;
    }

    @Override
    public void close() throws IOException {
        isOpen.set(false);
    }
}
