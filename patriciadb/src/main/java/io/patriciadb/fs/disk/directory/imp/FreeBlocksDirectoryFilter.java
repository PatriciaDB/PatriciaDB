package io.patriciadb.fs.disk.directory.imp;

import io.patriciadb.fs.TaskScheduledExecutor;
import io.patriciadb.fs.disk.DirectoryError;
import io.patriciadb.fs.disk.directory.Directory;
import io.patriciadb.fs.disk.directory.FreeBlockIdSource;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FreeBlocksDirectoryFilter implements Directory, PatriciaController, FreeBlockIdSource {
    private final static Logger log = LoggerFactory.getLogger(FreeBlocksDirectoryFilter.class);

    private final Roaring64NavigableMap freeBlocksMap = new Roaring64NavigableMap(false);
    private final Directory nextDirectory;
    private final TaskScheduledExecutor taskScheduledExecutor;
    private final AtomicLong nextFreeBlockId = new AtomicLong(0);
    private ScheduledFuture<?> optimisedBitmapTask;

    public FreeBlocksDirectoryFilter(Directory nextDirectory, TaskScheduledExecutor taskScheduledExecutor) {
        this.nextDirectory = nextDirectory;
        this.taskScheduledExecutor = taskScheduledExecutor;
    }

    @Override
    public void initialize() throws Exception {
        log.info("Scanning directory for free blockIds");
        var blockIdConsumer = new BlockIdConsumer() {
            private long maxBlockId = 1;

            @Override
            public void consume(long blockId, long pointer) {
                this.maxBlockId = blockId;
                if (pointer == 0) freeBlocksMap.addLong(blockId);
            }
        };
        nextDirectory.forEach(blockIdConsumer);
        freeBlocksMap.runOptimize();
        nextFreeBlockId.set(blockIdConsumer.maxBlockId + 1);
        log.info("Found {} free blocks pointer", freeBlocksMap.getLongCardinality());
        optimisedBitmapTask = taskScheduledExecutor.getExecutorService().scheduleWithFixedDelay(freeBlocksMap::runOptimize, 1, 2, TimeUnit.MINUTES);
    }

    @Override
    public Roaring64NavigableMap getFreeBlockId() {
        Roaring64NavigableMap copy = new Roaring64NavigableMap(false);
        copy.or(freeBlocksMap);
        return copy;
    }

    @Override
    public void preDestroy() throws Exception {
        if (optimisedBitmapTask != null) {
            optimisedBitmapTask.cancel(false);
        }
    }

    @Override
    public void expandCapacity() throws DirectoryError {
        long expandFrom = nextFreeBlockId.get();
        long expandTo = nextFreeBlockId.addAndGet(10_000_000);
        log.trace("Expanding from {} to {} (exclusive)", expandFrom, expandTo);
        freeBlocksMap.add(expandFrom, expandTo);
    }

    @Override
    public long get(long blockId) throws DirectoryError {
        return nextDirectory.get(blockId);
    }

    @Override
    public void forEach(BlockIdConsumer consumer) {
        nextDirectory.forEach(consumer);
    }

    @Override
    public synchronized void set(LongLongHashMap changeMap) throws DirectoryError {
        changeMap.forEachKeyValue((blockId, pointer) -> {
            if (pointer == 0) {
                freeBlocksMap.addLong(blockId);
            } else {
                freeBlocksMap.removeLong(blockId);
            }
        });
        nextDirectory.set(changeMap);
    }
}
