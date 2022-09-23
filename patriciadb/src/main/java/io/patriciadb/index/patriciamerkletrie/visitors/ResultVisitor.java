package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;

public interface ResultVisitor<T> {
    T apply(BranchNode branch);

    T apply(ExtensionNode extension);

    T apply(LeafNode leaf);

    T apply(EmptyNode empty);
}
