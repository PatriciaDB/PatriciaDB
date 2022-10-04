package io.patriciadb.index.patriciamerkletrie.format.plain;

import io.patriciadb.index.patriciamerkletrie.format.Format;
import io.patriciadb.index.patriciamerkletrie.format.Header;
import io.patriciadb.index.patriciamerkletrie.format.HeaderNodePair;
import io.patriciadb.index.patriciamerkletrie.format.StorageSerializer;
import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.nodes.Node;
import io.patriciadb.index.patriciamerkletrie.nodes.NodeState;
import io.patriciadb.index.patriciamerkletrie.serializer.NodeDeserialiser;

import java.nio.ByteBuffer;

public class PlainFormat implements Format {

    public static final PlainFormat INSTANCE = new PlainFormat();

    private final LocalStorageSerializer storageSerializer = new LocalStorageSerializer();

    private PlainFormat() {

    }

    @Override
    public byte[] generateRootHash(NodeLoader nodeLoader, Node root) {
        throw new UnsupportedOperationException("Plain Format don't support hash calculation");
    }

    @Override
    public boolean isNodeHashingSupported() {
        return false;
    }


    @Override
    public StorageSerializer storageSerializer() {
        return storageSerializer;
    }


    private static class LocalStorageSerializer implements StorageSerializer {
        @Override
        public ByteBuffer packWithHeader(Node node, byte[] serialisedNodeData) {
            return ByteBuffer.wrap(serialisedNodeData);
        }

        @Override
        public HeaderNodePair unpackNodeWithHeader(long nodeId, ByteBuffer buffer) {
            var node = (Node)NodeDeserialiser.deserialise(buffer);
            node.setState(NodeState.PERSISTED);
            node.setNodeId(nodeId);
            return new HeaderNodePair() {
                @Override
                public Header getHeader() {
                    return null;
                }

                @Override
                public Node getNode() {
                    return node;
                }
            };
        }

        @Override
        public HeaderNodePair headerNodePair(Node node) {
            return new HeaderNodePair() {
                @Override
                public Header getHeader() {
                    return null;
                }

                @Override
                public Node getNode() {
                    return node;
                }
            };
        }
    }
}
