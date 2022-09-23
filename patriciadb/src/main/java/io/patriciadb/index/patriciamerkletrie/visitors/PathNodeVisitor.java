package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;
import io.patriciadb.index.patriciamerkletrie.utils.Nibble;

public interface PathNodeVisitor<T> {
    T apply(BranchNode branch, Nibble nibble);

    T apply(ExtensionNode extension, Nibble nibble);

    T apply(LeafNode leaf, Nibble nibble);

    T apply(EmptyNode empty, Nibble nibble);
}
