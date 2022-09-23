package io.patriciadb.index.patriciamerkletrie.io;

import io.patriciadb.fs.BlockReader;
import io.patriciadb.index.patriciamerkletrie.format.HeaderNodePair;
import io.patriciadb.index.patriciamerkletrie.format.StorageSerializer;
import io.patriciadb.index.patriciamerkletrie.nodes.Node;

public class NodeLoader {

    private final BlockReader blockReader;
    private final StorageSerializer storageSerializer;

    public NodeLoader(BlockReader blockReader, StorageSerializer storageSerializer) {
        this.blockReader = blockReader;
        this.storageSerializer = storageSerializer;
    }

    public Node loadNode(Object val) {
        if (val instanceof Long id) {
            return loadFromStorage(id).getNode();
        } else if (val instanceof Node node) {
            return node;
        }
        throw new IllegalArgumentException("Val is not a Long or Node. Found " + val);
    }

    public HeaderNodePair loadHashNodePair(Object val) {
        if (val instanceof Long id) {
            return loadFromStorage(id);
        } else if (val instanceof Node node) {
            return storageSerializer.headerNodePair(node);
        }
        throw new IllegalArgumentException("Val is not a Long or Node. Found " + val);
    }

    public HeaderNodePair loadFromStorage(long nodeId) {
        var buffer = blockReader.read(nodeId);
        if (buffer == null) {
            throw new IllegalArgumentException("Node " + nodeId + " not found");
        }
        return storageSerializer.unpackNodeWithHeader(nodeId, buffer);
    }

}
