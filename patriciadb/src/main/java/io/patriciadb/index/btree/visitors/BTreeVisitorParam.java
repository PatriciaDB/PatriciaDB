package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;

public interface BTreeVisitorParam<E extends Comparable<E>, P, T> {


    T apply(InternalNode<E> internalNode, P param);


    T apply(LeafNode<E> leafNode, P param);

}
