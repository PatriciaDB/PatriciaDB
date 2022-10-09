package io.patriciadb.fs.disk.transaction;

import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class BatchUpdate implements Batch{
    private final Roaring64NavigableMap deletedBlocks = new Roaring64NavigableMap(false);
    private final LongLongHashMap newBlocksMap = new LongLongHashMap();
    private final LongLongHashMap updatedBlocksMap = new LongLongHashMap();

    public boolean contains(long blockId) {
        return deletedBlocks.contains(blockId)
                || newBlocksMap.containsKey(blockId)
                || updatedBlocksMap.containsKey(blockId);
    }

    public long get(long blockId) {
        if(deletedBlocks.contains(blockId)) {
            return 0;
        } else if(newBlocksMap.containsKey(blockId)) {
            return newBlocksMap.get(blockId);
        } else if(updatedBlocksMap.containsKey(blockId)) {
            return updatedBlocksMap.get(blockId);
        } else {
            return 0;
        }
    }

    public Roaring64NavigableMap getDeletedBlocks() {
        return deletedBlocks;
    }

    public LongLongHashMap getNewBlocks() {
        return newBlocksMap;
    }

    public LongLongHashMap getUpdatedBlocks() {
        return updatedBlocksMap;
    }

    public void delete(long blockId) {
        if(newBlocksMap.containsKey(blockId)) {
            newBlocksMap.remove(blockId);
        } else {
            deletedBlocks.add(blockId);
            updatedBlocksMap.remove(blockId);
        }
    }

    public void update(long blockId, long pointer) {
        deletedBlocks.removeLong(blockId);
        if(newBlocksMap.containsKey(blockId)) {
            newBlocksMap.put(blockId, pointer);
        } else {
            updatedBlocksMap.put(blockId, pointer);
        }
    }

    public void addNew(long blockId, long pointer) {
        deletedBlocks.removeLong(blockId);
        if(updatedBlocksMap.containsKey(blockId)) {
            throw new IllegalStateException();
        }
        newBlocksMap.put(blockId, pointer);
    }


}
