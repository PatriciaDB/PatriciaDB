package io.patriciadb.index.patriciamerkletrie.format.eth;

import io.patriciadb.index.patriciamerkletrie.format.HeaderNodePair;
import io.patriciadb.index.patriciamerkletrie.format.StorageSerializer;
import io.patriciadb.index.patriciamerkletrie.nodes.Node;
import io.patriciadb.index.patriciamerkletrie.nodes.NodeState;
import io.patriciadb.index.patriciamerkletrie.serializer.NodeDeserialiser;

import java.nio.ByteBuffer;
import java.util.Optional;

public class EthStorageSerializer implements StorageSerializer {

    private final static int HEADER_NODE_WITH_HASH = 1;
    private final static int HEADER_NODE_WITHOUT_HASH = 2;

    private final int hashLength;
    private final EthNodeHeader EMPTY_HEADER = () -> Optional.empty();

    public EthStorageSerializer(int hashLength) {
        this.hashLength = hashLength;
    }

    @Override
    public HeaderNodePair headerNodePair(Node node) {
        byte[] hash = node.getHash();
        EthNodeHeader header = hash == null ? EMPTY_HEADER : new EthNodeHeaderImp(hash);
        return new EthHeaderNodePairBasic(header, node);
    }

    @Override
    public ByteBuffer packWithHeader(Node node, byte[] data) {
        ByteBuffer buffer;
        if (node.getHash() != null) {
            buffer = ByteBuffer.allocate(Byte.BYTES + node.getHash().length + data.length);
            buffer.put((byte) HEADER_NODE_WITH_HASH);
            buffer.put(node.getHash());
        } else {
            buffer = ByteBuffer.allocate(Byte.BYTES+data.length);
            buffer.put((byte) HEADER_NODE_WITHOUT_HASH);
        }
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    @Override
    public EthHeaderNodePair unpackNodeWithHeader(long nodeId, ByteBuffer buffer) {
        return new LazyHashNodePair(buffer, nodeId);
    }

    private static class EthHeaderNodePairBasic implements EthHeaderNodePair {
        private final EthNodeHeader header;
        private final Node node;

        public EthHeaderNodePairBasic(EthNodeHeader header, Node node) {
            this.header = header;
            this.node = node;
        }

        @Override
        public Node getNode() {
            return node;
        }

        @Override
        public EthNodeHeader getHeader() {
            return header;
        }
    }

    private  class LazyHashNodePair implements EthHeaderNodePair {
        private final ByteBuffer buffer;
        private final long nodeId;
        private final EthNodeHeader header;
        private Node node;

        public LazyHashNodePair(ByteBuffer buffer, long nodeId) {
            int headerVal = buffer.get();
            if(headerVal==HEADER_NODE_WITH_HASH) {
                byte[] hash = new byte[hashLength];
                buffer.get(hash);
                header = new EthNodeHeaderImp(hash);
            } else if(headerVal==HEADER_NODE_WITHOUT_HASH){
                header = EMPTY_HEADER;
            } else {
                throw new IllegalArgumentException("Invalid Header value: "+headerVal);
            }

            this.nodeId = nodeId;
            this.buffer = buffer;
        }

        @Override
        public EthNodeHeader getHeader() {
            return header;
        }

        @Override
        public Node getNode() {
            if(node ==null) {
                node =(Node) NodeDeserialiser.deserialise(buffer);
                if(header.getHash().isPresent()) {
                    node.setHash(header.getHash().get());
                }
                node.setNodeId(nodeId);
                node.setState(NodeState.PERSISTED);
            }
            return node;
        }
    }

    private static class EthNodeHeaderImp implements EthNodeHeader {
        private final Optional<byte[]> hash;

        public EthNodeHeaderImp(byte[] hash) {
            this.hash = Optional.of(hash);
        }

        @Override
        public Optional<byte[]> getHash() {
            return hash;
        }
    }

}
