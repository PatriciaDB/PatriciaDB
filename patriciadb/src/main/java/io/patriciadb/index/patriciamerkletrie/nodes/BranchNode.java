package io.patriciadb.index.patriciamerkletrie.nodes;

import io.patriciadb.index.patriciamerkletrie.utils.Nibble;
import io.patriciadb.index.patriciamerkletrie.visitors.PathNodeVisitor;
import io.patriciadb.index.patriciamerkletrie.visitors.Visitor;
import io.patriciadb.utils.Pair;
import io.patriciadb.index.patriciamerkletrie.visitors.ResultVisitor;

import java.util.Objects;

public class BranchNode extends Node {
    private final Object[] nodes;

    public BranchNode(Object[] nodes) {
        this.nodes = nodes;
    }

    public static BranchNode createNew() {
        return new BranchNode(new Object[17]);
    }

    @Override
    public  <T> void apply(Visitor<T> visitor, T val) {
        visitor.apply(this, val);
    }

    @Override
    public <T> T apply(PathNodeVisitor<T> visitor, Nibble nibble) {
        return visitor.apply(this, nibble);
    }

    @Override
    public <T> T apply(ResultVisitor<T> visitor) {
        return visitor.apply(this);
    }

    public BranchNode replaceChild(int i, Object node) {
        if (i < 0 || i > 16) {
            throw new IllegalArgumentException("Invalid child: " + i);
        }
        var newNodes = nodes.clone();
        newNodes[i] = node;
        return new BranchNode(newNodes);
    }

    public BranchNode clearChild(int i) {
        if (i < 0 || i > 16) {
            throw new IllegalArgumentException("Invalid child: " + i);
        }
        var newNodes = nodes.clone();
        newNodes[i] = null;
        return new BranchNode(newNodes);
    }
    public BranchNode insert(Nibble nibble, Object node) {
        if (nibble.isEmpty()) {
            throw new IllegalArgumentException("Empty nibble");
        }
        int bucket = nibble.get(0);
        var newNibble = nibble.removePrefix(1);
        if (newNibble.isEmpty()) {
            return replaceChild(bucket, node);
        }
        return replaceChild(bucket, new ExtensionNode(newNibble, node));
    }

    public BranchNode replaceValue(Object leaf) {
        var newNodes = nodes.clone();
        newNodes[16] = Objects.requireNonNull(leaf);
        return new BranchNode(newNodes);
    }

    public Pair<Nibble, Object> normaliseBranch() {
        int count = 0;
        int lastIndexFound = -1;
        for (int i = 0; i < 17; i++) {
            if (nodes[i] != null) {
                count++;
                lastIndexFound = i;
            }
        }
        if (count == 0) {
            return new Pair<>(Nibble.EMPTY, EmptyNode.INSTANCE);
        }
        if (count > 1) {
            return new Pair<>(Nibble.EMPTY, this);
        }
        if (lastIndexFound == 16) {
            return new Pair<>(Nibble.EMPTY, nodes[lastIndexFound]);
        } else {
            return new Pair<>(Nibble.EMPTY.append(lastIndexFound), nodes[lastIndexFound]);
        }
    }

    public BranchNode clearValue() {
        if (nodes[16] == null) {
            return this;
        }
        var newNodes = nodes.clone();
        newNodes[16] = null;
        return new BranchNode(newNodes);
    }

    public Object getChild(int i) {
        return nodes[i];
    }

    public Object getValue() {
        return nodes[16];
    }
}
