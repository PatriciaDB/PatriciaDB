package io.patriciadb.index.patriciamerkletrie.format;

import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.nodes.Node;

public interface Format {
    Hasher hasher();

    byte[] generateRootHash(NodeLoader nodeLoader, Node root);

    StorageSerializer storageSerializer();

}
