package io.patriciadb.index.patriciamerkletrie.format;

import io.patriciadb.index.patriciamerkletrie.nodes.Node;

public interface HeaderNodePair {
    Header getHeader();

    Node getNode();
}
