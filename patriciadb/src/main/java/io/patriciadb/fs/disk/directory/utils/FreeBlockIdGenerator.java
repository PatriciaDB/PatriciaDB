package io.patriciadb.fs.disk.directory.utils;


import io.patriciadb.fs.disk.directory.FreeBlockIdSource;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class FreeBlockIdGenerator  {

    private final FreeBlockIdSource freeBlockIdSource;
    private final Roaring64NavigableMap provided;
    private volatile Roaring64NavigableMap currentSet;
    private  volatile LongIterator longIterator;

    public FreeBlockIdGenerator(FreeBlockIdSource freeBlockIdSource) {
        this.freeBlockIdSource = freeBlockIdSource;
        currentSet= freeBlockIdSource.getFreeBlockId();
        provided = new Roaring64NavigableMap(false);
        provided.or(currentSet);
        longIterator =currentSet.getLongIterator();
    }

    public long nextBlockId() {
        if(longIterator.hasNext()) {
            return longIterator.next();
        }
        freeBlockIdSource.expandCapacity();
        var newFreeBlockIdSet = freeBlockIdSource.getFreeBlockId();
        newFreeBlockIdSet.andNot(provided);
        provided.or(newFreeBlockIdSet);
        currentSet = newFreeBlockIdSet;
        longIterator = currentSet.getLongIterator();
        if(!longIterator.hasNext()) {
            throw new IllegalStateException("FreeBlockIds is zero cardinality after the expansion");
        }
        return longIterator.next();
    }
}
