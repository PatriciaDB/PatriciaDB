package io.patriciadb.index.patriciamerkletrie.serializer;

import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;
import io.patriciadb.index.patriciamerkletrie.utils.Nibble;
import io.patriciadb.utils.VarInt;

import java.nio.ByteBuffer;

public class NodeDeserialiser {

    public static Object deserialise(ByteBuffer b) {
        var node = deserialiseInternal(b);
        return node!=null ? node : EmptyNode.INSTANCE;
    }
    private static Object deserialiseInternal(ByteBuffer b) {
        int type = b.get();
        if (type == Constants.LEAF_NODE_HEADER) {
            return deserialiseLeaf(b);
        } else if (type == Constants.EXTENSION_NODE_HEADER) {
            return deserialiseExtension(b);
        } else if (type == Constants.LINK_NODE_HEADER) {
            return VarInt.getVarLong16(b);
        } else if (type == Constants.BRANCH_NODE_HEADER) {
            return deserialiseBranch(b);
        } else if (type == Constants.NULL_NODE_HEADER) {
            return null;
        }
        throw new IllegalStateException("Type unknown " + type);
    }

    private static ExtensionNode deserialiseExtension(ByteBuffer b) {
        var nibble = Nibble.deserializeWithHeader(b);
        var nextnode = deserialiseInternal(b);
        return new ExtensionNode(nibble, nextnode);
    }

    private static LeafNode deserialiseLeaf(ByteBuffer b) {
        int length = VarInt.getVarInt(b);
        var data = new byte[length];
        b.get(data);
        return new LeafNode(data);
    }

    private static BranchNode deserialiseBranch(ByteBuffer b) {
        var nodes = new Object[17];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = deserialiseInternal(b);
        }
        return new BranchNode(nodes);
    }
}
