package io.patriciadb.index.patriciamerkletrie.serializer;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.index.patriciamerkletrie.PersistedNodeObserver;
import io.patriciadb.index.patriciamerkletrie.format.StorageSerializer;
import io.patriciadb.index.patriciamerkletrie.nodes.*;
import io.patriciadb.index.patriciamerkletrie.visitors.ResultVisitor;
import io.patriciadb.utils.VarInt;
import org.bouncycastle.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class SerializerPersisterVisitor implements ResultVisitor<byte[]> {

    private final BlockWriter blockWriter;
    private final StorageSerializer storageSerializer;
    private final PersistedNodeObserver persistedNodeObserver;

    private SerializerPersisterVisitor(BlockWriter writer, StorageSerializer storageSerializer, PersistedNodeObserver persistedNodeObserver) {
        this.blockWriter = writer;
        this.storageSerializer = storageSerializer;
        this.persistedNodeObserver = persistedNodeObserver;
    }

    public static long persist(BlockWriter blockWriter,
                               StorageSerializer storageSerializer,
                               Node node,
                               PersistedNodeObserver persistedNodeObserver) {
        var serializerPersisterVisitor = new SerializerPersisterVisitor(blockWriter, storageSerializer, persistedNodeObserver);
        var nodeData = node.apply(serializerPersisterVisitor);
        return serializerPersisterVisitor.persistNode(node, nodeData);
    }

    @Override
    public byte[] apply(BranchNode branch) {
        var bos = new ByteArrayOutputStream(128);
        bos.write(Constants.BRANCH_NODE_HEADER);
        for (int i = 0; i < 16; i++) {
            var nodeRef = branch.getChild(i);
            if (nodeRef == null) {
                bos.write(Constants.NULL_NODE_HEADER);
            } else {
                appendNode(bos, nodeRef);
            }
        }
        if (branch.getValue() != null) {
            appendNode(bos, branch.getValue());
        } else {
            bos.write(Constants.NULL_NODE_HEADER);
        }

        return bos.toByteArray();
    }

    @Override
    public byte[] apply(ExtensionNode extension) {
        var b = new ByteArrayOutputStream();
        b.write(Constants.EXTENSION_NODE_HEADER);
        extension.getNibble().serializeWithHeader(b);
        var nextNodeRef = extension.getNextNode();
        appendNode(b, nextNodeRef);
        return b.toByteArray();
    }

    @Override
    public byte[] apply(LeafNode leaf) {
        var payload = leaf.getValue();
        ByteBuffer buffer = ByteBuffer.allocate(Bytes.BYTES + VarInt.varIntSize(payload.length) + payload.length);
        buffer.put(Constants.LEAF_NODE_HEADER);
        VarInt.putVarInt(payload.length, buffer);
        buffer.put(payload);
        return buffer.array();
    }

    @Override
    public byte[] apply(EmptyNode empty) {
        return new byte[1];
    }

    private void appendNode(ByteArrayOutputStream bos, Object nextNodeRef) {
        if (nextNodeRef instanceof Long nodeId) {
            bos.write(Constants.LINK_NODE_HEADER);
            VarInt.putVarLong16(nodeId, bos);

        } else if (nextNodeRef instanceof Node nextNode) {
            if (nextNode.getState() == NodeState.PERSISTED) {
                bos.write(Constants.LINK_NODE_HEADER);
                VarInt.putVarLong16(nextNode.getNodeId(), bos);
            } else {
                var nextNodeData = nextNode.apply(this);
                if (nextNodeData.length > 32) {
                    long nodeId = persistNode(nextNode, nextNodeData);
                    bos.write(Constants.LINK_NODE_HEADER);
                    nextNode.setNodeId(nodeId);
                    VarInt.putVarLong16(nodeId, bos);
                } else {
                    bos.writeBytes(nextNodeData);
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown child type. Found " + nextNodeRef.getClass());
        }
    }


    private long persistNode(Node node, byte[] data) {
        if(node.getState() == NodeState.PERSISTED) {
            return node.getNodeId();
        }
        var buffer = storageSerializer.packWithHeader(node, data);
        if (buffer.remaining() == 0) {
            throw new IllegalArgumentException("Invalid buffer length 0");
        }
        var id = blockWriter.write(buffer);
        node.setNodeId(id);
        node.setState(NodeState.PERSISTED);
        if (persistedNodeObserver != null) {
            persistedNodeObserver.newNodePersisted(node);
        }
        return id;
    }
}
