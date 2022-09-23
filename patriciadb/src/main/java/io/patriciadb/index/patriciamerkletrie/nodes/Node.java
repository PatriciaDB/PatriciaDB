package io.patriciadb.index.patriciamerkletrie.nodes;

import io.patriciadb.index.patriciamerkletrie.utils.Nibble;
import io.patriciadb.index.patriciamerkletrie.visitors.PathNodeVisitor;
import io.patriciadb.index.patriciamerkletrie.visitors.Visitor;
import io.patriciadb.index.patriciamerkletrie.visitors.ResultVisitor;

public abstract class Node {

    private byte[] hash;
    private NodeState state;
    private Long nodeId;

    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    public abstract <T> T apply(PathNodeVisitor<T> visitor, Nibble nibble);

    public abstract <T> void apply(Visitor<T> visitor, T val);

    public abstract <T> T apply(ResultVisitor<T> visitor);

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }

    public byte[] getHash() {
        return hash;
    }

    public void setHash(byte[] hash) {
        this.hash = hash;
    }

    public boolean isEmptyNode(){
        return false;
    }

}
