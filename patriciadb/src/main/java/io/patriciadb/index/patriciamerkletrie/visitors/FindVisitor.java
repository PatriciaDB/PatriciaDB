package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;
import io.patriciadb.index.patriciamerkletrie.utils.Nibble;

public class FindVisitor implements PathNodeVisitor<byte[]> {
    private final NodeLoader nodeLoader;

    public FindVisitor(NodeLoader nodeLoader) {
        this.nodeLoader = nodeLoader;
    }

    @Override
    public byte[] apply(BranchNode branch, Nibble nibble) {
        if (nibble.isEmpty()) {
            if (branch.getValue() == null) {
                return null;
            }
            return nodeLoader.loadNode(branch.getValue()).apply(this, nibble);
        }
        int bucket = nibble.get(0);
        if (branch.getChild(bucket) == null) {
            return null;
        }

        return nodeLoader.loadNode(branch.getChild(bucket))
                .apply(this, nibble.removePrefix(1));
    }

    @Override
    public byte[] apply(ExtensionNode extension, Nibble nibble) {
        if (nibble.size() < extension.getNibble().size()) {
            return null;
        }
        if (!nibble.startWith(extension.getNibble())) {
            return null;
        }
        var newNibble = nibble.removePrefix(extension.getNibble().size());
        return nodeLoader.loadNode(extension.getNextNode()).apply(this, newNibble);
    }

    @Override
    public byte[] apply(LeafNode leaf, Nibble nibble) {
        if (!nibble.isEmpty()) {
            return null;
        }
        return leaf.getValue();
    }

    @Override
    public byte[] apply(EmptyNode empty, Nibble nibble) {
        return null;
    }
}
