package io.patriciadb.index.btree.serialize;

import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;
import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class BTreeNodeDeserializer<E extends Comparable<E>> {
    private final Serializer<E> entrySerializer;

    public BTreeNodeDeserializer(Serializer<E> entrySerializer) {
        this.entrySerializer = entrySerializer;
    }

    public BTreeNode<E> deserialise(ByteBuffer buffer) {
        int header = buffer.get();
        if (header == BTreeConstants.LEAF_HEADER) {
            var size = VarInt.getVarLong(buffer);
            var entries = new ArrayList<E>();
            for (int i = 0; i < size; i++) {
                var entry = entrySerializer.deserialize(buffer);
                entries.add(entry);
            }
            return new LeafNode<>(entries);
        } else if (header == BTreeConstants.INTERNAL_NODE_HEADER) {
            int depth = VarInt.getVarInt(buffer);
            var size = VarInt.getVarInt(buffer);
            ArrayList<Object> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                if (i % 2 == 0) {
                    long nodeId = VarInt.getVarLong16(buffer);
                    values.add(nodeId);
                } else {
                    var entry = entrySerializer.deserialize(buffer);
                    values.add(entry);
                }
            }
            return new InternalNode<>(values, depth);
        } else {
            throw new IllegalStateException("Invalid node header found "+header);
        }
    }
}
