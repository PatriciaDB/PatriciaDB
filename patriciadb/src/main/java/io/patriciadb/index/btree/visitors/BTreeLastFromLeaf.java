package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;

public class BTreeLastFromLeaf<E extends Comparable<E>> implements BTreeVisitor<E, E> {

    @Override
    public E apply(InternalNode<E> internalNode) {
        return null;
    }

    @Override
    public E apply(LeafNode<E> leafNode) {
        return null;
    }
}
