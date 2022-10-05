package io.patriciadb.fs.simple;

import io.patriciadb.fs.FsReadTransaction;
import io.patriciadb.fs.FsWriteTransaction;
import io.patriciadb.utils.VarInt;
import io.patriciadb.fs.PatriciaFileSystem;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple File System implements a basic in-memory file system. Snapshots reads are not supported
 */
public class SimpleFileSystem implements PatriciaFileSystem {
    private final ConcurrentHashMap<Long, ByteBuffer> data = new ConcurrentHashMap<>();
    private final AtomicLong blockIdGenerator = new AtomicLong(1000);

    public SimpleFileSystem() {

    }

    @Override
    public void runVacuum() {

    }

    public SimpleFileSystem(long firstBlockId) {
        blockIdGenerator.set(firstBlockId);
    }


    public long getBlockCount() {
        return data.size();
    }

    @Override
    public void close() {
        data.clear();
    }

    public long getDataSize() {
        return data.reduceEntriesToLong(2, (entry) -> {
            return VarInt.varLongSize(entry.getKey()) + entry.getValue().remaining();
        }, 0, Long::sum);
    }

    @Override
    public FsReadTransaction getSnapshot() {
        return new LocalReadTransaction();
    }

    @Override
    public FsWriteTransaction startTransaction() {
        return new LocalTransaction();
    }

    private class LocalReadTransaction implements FsReadTransaction {


        @Override
        public void release() {

        }

        @Override
        public ByteBuffer read(long blockId) {
            var buffer = data.get(blockId);
            if (buffer == null) {
                return null;
            }
            return buffer.duplicate();
        }
    }

    private class LocalTransaction implements FsWriteTransaction {
        private final ConcurrentHashMap<Long, Optional<ByteBuffer>> localCopy = new ConcurrentHashMap<>();

        @Override
        public ByteBuffer read(long blockId) {
            var elem = localCopy.get(blockId);
            if (elem != null) {
                return elem.map(ByteBuffer::duplicate).orElse(null);
            }
            var buffer = data.get(blockId);
            if (buffer == null) {
                return null;
            }
            return buffer.duplicate();
        }

        @Override
        public long write(ByteBuffer buffer) {
            long id = blockIdGenerator.getAndIncrement();
            ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
            copy.put(buffer);
            copy.flip();
            localCopy.put(id, Optional.of(copy));
            return id;
        }

        @Override
        public void overwrite(long blockId, ByteBuffer buffer) {
            localCopy.put(blockId, Optional.of(buffer.duplicate()));
        }

        @Override
        public void delete(long blockId) {
            localCopy.put(blockId, Optional.empty());
        }


        @Override
        public void release() {
            localCopy.clear();
        }

        @Override
        public void commit() {
            for (var entry : localCopy.entrySet()) {
                var key = entry.getKey();
                var value = entry.getValue();
                if (value.isEmpty()) {
                    data.remove(key);
                } else {
                    data.put(key, value.get());
                }
            }
        }
    }
}
