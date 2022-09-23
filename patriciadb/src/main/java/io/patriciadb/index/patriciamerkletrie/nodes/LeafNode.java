package io.patriciadb.index.patriciamerkletrie.nodes;

import io.patriciadb.index.patriciamerkletrie.utils.Nibble;
import io.patriciadb.index.patriciamerkletrie.visitors.PathNodeVisitor;
import io.patriciadb.index.patriciamerkletrie.visitors.Visitor;
import io.patriciadb.index.patriciamerkletrie.visitors.ResultVisitor;

public class LeafNode extends Node {
    private final byte[] value;

    public LeafNode(byte[] value) {
        this.value = value;
    }

    @Override
    public  <T> void apply(Visitor<T> visitor, T val) {
        visitor.apply(this, val);
    }

    @Override
    public <T> T apply(ResultVisitor<T> visitor) {
        return visitor.apply(this);
    }

    @Override
    public <T> T apply(PathNodeVisitor<T> visitor, Nibble nibble) {
        return visitor.apply(this, nibble);
    }

    public byte[] getValue() {
        return value;
    }
}
