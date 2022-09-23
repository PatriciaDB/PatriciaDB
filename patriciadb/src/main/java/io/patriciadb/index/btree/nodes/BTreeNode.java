package io.patriciadb.index.btree.nodes;

import io.patriciadb.index.btree.visitors.BTreeVisitor;
import io.patriciadb.index.btree.visitors.BTreeVisitorParam;

public abstract class BTreeNode<E extends Comparable<E>> {
    private long nodeId;
    private BTreeNodeState nodeState = BTreeNodeState.NEW;

    public BTreeNodeState getNodeState() {
        return nodeState;
    }

    public void setNodeState(BTreeNodeState nodeState) {
        this.nodeState = nodeState;
    }

    public long getNodeId() {
        return nodeId;
    }

    public void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    public abstract <T> T apply(BTreeVisitor<E, T> visitor);

    public abstract <P,T> T apply(BTreeVisitorParam<E, P,T> visitor, P entry);

    public abstract int entrySize();

    public abstract int getDepth();
}
