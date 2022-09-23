package io.patriciadb.index.patriciamerkletrie.format;

import io.patriciadb.index.patriciamerkletrie.nodes.Node;

import java.nio.ByteBuffer;

public interface StorageSerializer {
    ByteBuffer packWithHeader(Node node, byte[] serialisedNodeData);

    HeaderNodePair unpackNodeWithHeader(long nodeId, ByteBuffer buffer);

    HeaderNodePair headerNodePair(Node node);
}
