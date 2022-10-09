package io.patriciadb.fs.disk.transaction;

import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public interface Batch {

     Roaring64NavigableMap getDeletedBlocks();;

     LongLongHashMap getNewBlocks() ;

     LongLongHashMap getUpdatedBlocks() ;
}
