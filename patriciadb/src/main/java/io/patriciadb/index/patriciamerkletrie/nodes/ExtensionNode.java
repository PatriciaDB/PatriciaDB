package io.patriciadb.index.patriciamerkletrie.nodes;

import io.patriciadb.index.patriciamerkletrie.utils.Nibble;
import io.patriciadb.index.patriciamerkletrie.visitors.PathNodeVisitor;
import io.patriciadb.index.patriciamerkletrie.visitors.Visitor;
import io.patriciadb.index.patriciamerkletrie.visitors.ResultVisitor;

public class ExtensionNode extends Node {
    private final Nibble nibble;
    private final Object node;

    public ExtensionNode(Nibble nibble, Object node) {
        this.nibble = nibble;
        this.node = node;
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

    public Object removeFirstBucket() {
        if(nibble.size()<=1) {
            return node;
        } else {
            return new ExtensionNode(nibble.removePrefix(1), node);
        }
    }

    public Nibble getNibble() {
        return nibble;
    }

    public Object getNextNode() {
        return node;
    }


}
