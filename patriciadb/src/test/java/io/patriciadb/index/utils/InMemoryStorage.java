package io.patriciadb.index.utils;

import io.patriciadb.fs.BlockWriter;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryStorage implements BlockWriter {
    private final HashMap<Long, ByteBuffer> data = new HashMap<>();
    private final AtomicLong idgenerator = new AtomicLong(1);

    public long write(ByteBuffer buffer) {
        var id = idgenerator.getAndIncrement();
        overwrite(id, buffer);
        return id;
    }

    public void overwrite(long blockId, ByteBuffer buffer) {

        ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
        copy.put(buffer);
        copy.flip();
        data.put(blockId, copy);
    }


    @Override
    public ByteBuffer read(long blockId) {
        return get(blockId);
    }

    public void delete(long blockId) {
        data.remove(blockId);
    }

    public long nodeCount() {
        return data.size();
    }

    public long totalSize() {
        long c = 0;
        for (var value : data.values()) {
            c += value.remaining();
        }
        return c;
    }

    public ByteBuffer get(long id) {
        var buffer = data.get(id);
        if(buffer==null) {
            return null;
        }
        return buffer.duplicate();
    }
}
