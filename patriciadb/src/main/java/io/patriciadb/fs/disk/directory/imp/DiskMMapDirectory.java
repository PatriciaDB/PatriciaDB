package io.patriciadb.fs.disk.directory.imp;


import io.patriciadb.fs.disk.DirectoryError;
import io.patriciadb.fs.disk.directory.DiskDirectory;
import io.patriciadb.fs.disk.directory.utils.SegmentUtils;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

public class DiskMMapDirectory implements DiskDirectory, PatriciaController {

    private final Logger log = LoggerFactory.getLogger(DiskMMapDirectory.class);
    private static final long ROW_SIZE = Long.BYTES;
    public static final long MAX_BLOCKS_PER_SEGMENT = Integer.MAX_VALUE / ROW_SIZE;
    public static final long MAX_SEGMENT_SIZE = MAX_BLOCKS_PER_SEGMENT * ROW_SIZE;
    private static final long MINIMUM_DIRECTORY_CAPACITY = 1_000_000;
    private static final long INCREASE_DELTA_CAPACITY = 3_000_000;
    private static final long MINIMUM_DIRECTORY_CAPACITY_SIZE = MINIMUM_DIRECTORY_CAPACITY * ROW_SIZE;

    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final FileChannel ch;
    private MappedByteBuffer[] buffers;
    private long currentBlockCapacity;

    public DiskMMapDirectory(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        this.ch = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Load segments
        long fileSize = ch.size();
        if (fileSize < MINIMUM_DIRECTORY_CAPACITY_SIZE) {
            fileSize = MINIMUM_DIRECTORY_CAPACITY_SIZE;
        }
        long blockIdCount = fileSize / ROW_SIZE;
        remapFile(blockIdCount);
    }

    @Override
    public void forEach(BlockIdConsumer consumer) {
        checkState();
        for (long i = 1; i < currentBlockCapacity; i++) {
            consumer.consume(i, getUnsafe(i));
        }
    }

    private void remapFile(long newBlockCapacity) throws DirectoryError {
        try {
            long oldCapacity = currentBlockCapacity;
            if (newBlockCapacity < oldCapacity) {
                throw new IllegalArgumentException("Directory cannot be remapped with a smaller size");
            }
            log.trace("Expanding Disk directory to new capacity of {}", newBlockCapacity);
            var segments = SegmentUtils.calculateSegments(newBlockCapacity * ROW_SIZE, MAX_SEGMENT_SIZE);
            buffers = new MappedByteBuffer[segments.size()];
            for (int i = 0; i < segments.size(); i++) {
                var segment = segments.get(i);
                var mmapSegment = ch.map(MapMode.READ_WRITE, segment.initialPosition(), segment.length());
                buffers[i] = mmapSegment;
            }
            currentBlockCapacity = newBlockCapacity;
        } catch (IOException e) {
            log.error("Failed to remap disk directory", e);
            throw new DirectoryError(true, "Error while ReMapping file", e);
        }
    }

    private void set(long blockId, long blockPosition) throws DirectoryError {
        if (blockPosition == 0) {
            throw new IllegalArgumentException("Invalid blockPosition zero");
        }
        if (blockId >= currentBlockCapacity) {
            expandToBlock(blockId + INCREASE_DELTA_CAPACITY);
        }
        setUnsafe(blockId, blockPosition);
    }

    @Override
    public synchronized void set(LongLongHashMap changeMap) {
        checkState();
        for (var entry : changeMap.keyValuesView()) {
            long k = entry.getOne();
            long v = entry.getTwo();
            if (v == 0) {
                clear(k);
            } else {
                set(k, v);
            }
        }
    }

    @Override
    public synchronized void sync() throws DirectoryError {
        checkState();

        try {
            for (var mmap : buffers) {
                mmap.force();
            }
        } catch (UncheckedIOException e) {
            throw new DirectoryError(true, e);
        }
    }

    public synchronized long get(long blockId) {
        checkState();
        if (blockId >= currentBlockCapacity) {
            return 0;
        }
        return getUnsafe(blockId);
    }

    public synchronized LongLongHashMap get(LongIterable ids) {
        checkState();
        LongLongHashMap map = new LongLongHashMap();
        ids.forEach(id -> map.put(id, get(id)));
        return map;
    }

    private void clear(long blockId) throws DirectoryError {
        if (blockId >= currentBlockCapacity) {
            return;
        }
        setUnsafe(blockId, 0);
    }

//    public synchronized boolean compareAndSet(long blockId, long expectedVal, long newValue) throws DirectoryError {
//        if (blockId >= currentBlockCapacity) {
//            expandToBlock(blockId + INCREASE_DELTA_CAPACITY);
//        }
//        long position = blockId * ROW_SIZE;
//        int mmapPosition = (int) (position % MAX_SEGMENT_SIZE);
//        int mmapIndex = (int) (position / MAX_SEGMENT_SIZE);
//        var mmap = buffers[mmapIndex];
//        var oldValue = mmap.getLong(mmapPosition);
//        if (oldValue != expectedVal) {
//            return false;
//        }
//        mmap.putLong(mmapPosition, newValue);
//        return true;
//    }

    private synchronized void expandToBlock(long expandToBlock) throws DirectoryError {
        remapFile(expandToBlock);
    }

    private long getUnsafe(long blockId) {
        long position = blockId * ROW_SIZE;
        int mmapIndex = (int) (position / MAX_SEGMENT_SIZE);
        int mmapPosition = (int) (position % MAX_SEGMENT_SIZE);
        var buffer = buffers[mmapIndex];
        return buffer.getLong(mmapPosition);
    }

    private void setUnsafe(long blockId, long blockPosition) {
        long position = blockId * ROW_SIZE;
        int mmapPosition = (int) (position % MAX_SEGMENT_SIZE);
        int mmapIndex = (int) (position / MAX_SEGMENT_SIZE);
        var buffer = buffers[mmapIndex];
        buffer.putLong(mmapPosition, blockPosition);
    }

    private void checkState() {
        if (!isOpen.get()) {
            throw new DirectoryError(true, "Directory is closed");
        }
    }

    @Override
    public synchronized void destroy() throws Exception {
        log.info("Closing Disk Directory");
        try {
            ch.close();
        } finally {
            isOpen.set(false);
        }
    }

}
