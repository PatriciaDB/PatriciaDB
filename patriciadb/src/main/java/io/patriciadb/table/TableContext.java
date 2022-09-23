package io.patriciadb.table;

import io.patriciadb.index.btree.BTree;
import io.patriciadb.utils.Serializer;

import java.util.List;

public interface TableContext<E extends Entity> {

    Serializer<E> entitySerializer();

    List<Index<E>> getIndexList();

    BTree<Long> getPrimaryIndex();

    long getTableId();
}
