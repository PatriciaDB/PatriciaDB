package io.patriciadb.index.btree.serialize;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.index.btree.io.BTreeNodeTracker;
import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.BTreeNodeState;
import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;
import io.patriciadb.index.btree.visitors.BTreeVisitor;
import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class BTreePersistChanges<E extends Comparable<E>> {

    private final static Logger log = LoggerFactory.getLogger(BTreePersistChanges.class);

    private final BlockWriter storage;
    private final Serializer<E> entrySerialiser;
    private final NodeSerializer serializer = new NodeSerializer();

    public BTreePersistChanges(BlockWriter storage, Serializer<E> entrySerialiser) {
        this.storage = storage;
        this.entrySerialiser = entrySerialiser;
    }

    public void persist(BTreeNodeTracker<E> nodeTracker) {
        var newNodesSortedByDepth = nodeTracker.getUpdatedNodes()
                .stream()
                .sorted(Comparator.comparing(BTreeNode::getDepth))
                .toList();
        for (var node : newNodesSortedByDepth) {
            var buffer = node.apply(serializer);
            if(node.getNodeState()== BTreeNodeState.NEW) {

                long nodeId = storage.write(buffer);
                node.setNodeState(BTreeNodeState.PERSISTED);
                node.setNodeId(nodeId);
            } else {
                storage.overwrite(node.getNodeId(), buffer);

            }
        }

        for (var nodeId : nodeTracker.getPendingDeleteNodes()) {
            storage.delete(nodeId);
        }
        nodeTracker.reset();
    }

    private class NodeSerializer implements BTreeVisitor<E, ByteBuffer> {

        @Override
        public ByteBuffer apply(InternalNode<E> internalNode) {
            var bos = new ByteArrayOutputStream();
            bos.write(BTreeConstants.INTERNAL_NODE_HEADER);
            VarInt.putVarInt(internalNode.getDepth(), bos);
            int size = internalNode.size();
            VarInt.putVarInt(size, bos);
            for (int i = 0; i < size; i++) {
                if (internalNode.isEntry(i)) {
                    entrySerialiser.serialize(internalNode.getEntry(i), bos);

                } else {
                    var childRef = internalNode.getChild(i);
                    try {
                        var childId = getNodeId(childRef);
                        VarInt.putVarLong16(childId, bos);
                    } catch (IllegalStateException ex) {
                        throw ex;
                    }
                }
            }
            return ByteBuffer.wrap(bos.toByteArray());
        }

        @Override
        public ByteBuffer apply(LeafNode<E> leafNode) {
            var bos = new ByteArrayOutputStream();
            bos.write(BTreeConstants.LEAF_HEADER);
            int size = leafNode.size();
            VarInt.putVarInt(size, bos);
            for (int i = 0; i < size; i++) {
                entrySerialiser.serialize(leafNode.getEntry(i), bos);
            }
            return ByteBuffer.wrap(bos.toByteArray());
        }

        private long getNodeId(Object childRef) {
            if (childRef instanceof Long nodeId) {
                return nodeId;
            } else if (childRef instanceof BTreeNode<?> node) {
                if (node.getNodeState() == BTreeNodeState.PERSISTED) {
                    return node.getNodeId();
                } else {
                    throw new IllegalStateException("Cannot serialise node as one the child is not persisted");
                }
            } else {
                throw new IllegalStateException("Unknown child type");
            }
        }
    }
}
