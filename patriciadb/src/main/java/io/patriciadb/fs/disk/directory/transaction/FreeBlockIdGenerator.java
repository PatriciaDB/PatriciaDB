package io.patriciadb.fs.disk.directory.transaction;


import io.patriciadb.fs.disk.directory.Directory;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class FreeBlockIdGenerator  {

    private final Directory directory;
    private final Roaring64NavigableMap provided;
    private volatile Roaring64NavigableMap currentSet;
    private  volatile LongIterator longIterator;

    public FreeBlockIdGenerator(Directory directory) {
        this.directory = directory;
        currentSet= directory.getFreeBlocksMap();
        provided = new Roaring64NavigableMap(false);
        provided.or(currentSet);
        longIterator =currentSet.getReverseLongIterator();
    }

    public long nextBlockId() {
        if(longIterator.hasNext()) {
            return longIterator.next();
        }
        directory.expandCapacity();
        var newFreeBlockIdSet = directory.getFreeBlocksMap();
        newFreeBlockIdSet.andNot(provided);
        provided.or(newFreeBlockIdSet);
        currentSet = newFreeBlockIdSet;
        longIterator = currentSet.getReverseLongIterator();
        if(!longIterator.hasNext()) {
            throw new IllegalStateException("FreeBlockIds is zero cardinality after the expansion");
        }
        return longIterator.next();
    }
}
