package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;

public interface Visitor<T> {
    void apply(BranchNode branch, T val);

    void apply(ExtensionNode extension, T val);

    void apply(LeafNode leaf, T val);

    void apply(EmptyNode empty, T val);
}
