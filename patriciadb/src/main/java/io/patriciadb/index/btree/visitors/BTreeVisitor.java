package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;

public interface BTreeVisitor<E extends Comparable<E>, T> {

     T apply(InternalNode<E> internalNode);


     T apply(LeafNode<E> leafNode);

}
