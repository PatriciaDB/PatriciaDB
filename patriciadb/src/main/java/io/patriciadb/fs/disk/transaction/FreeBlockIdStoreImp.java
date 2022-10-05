package io.patriciadb.fs.disk.transaction;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class FreeBlockIdStoreImp implements FreeBlockIdStore {
    private final Roaring64NavigableMap availableBlocks = new Roaring64NavigableMap(false);
    private long nextId;

    public FreeBlockIdStoreImp(Roaring64NavigableMap takenBlocks) {
        long cardinality = takenBlocks.getLongCardinality();
        if (cardinality == 0) {
            nextId = 1000;
        } else {
            this.nextId = takenBlocks.select(cardinality - 1) + 1;
            for (long i = 1000; i < nextId; i++) {
                if (!takenBlocks.contains(i)) {
                    availableBlocks.add(i);
                }
                availableBlocks.runOptimize();
            }
        }
    }

    @Override
    public synchronized long getNextFreeBlockId() {
        if (availableBlocks.getLongCardinality() == 0) {
            availableBlocks.add(nextId, nextId + 100_000);
            nextId += 100_000;
        }
        long val = availableBlocks.select(0);
        availableBlocks.removeLong(val);
        return val;
    }

    @Override
    public synchronized void addFreeBlockIds(Roaring64NavigableMap freeBlocks) {
        availableBlocks.or(freeBlocks);
    }


}
