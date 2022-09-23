package io.patriciadb.index.btree.io;

import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.BTreeNodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class BTreeNodeTracker<E extends Comparable<E>> {
    private final static Logger log = LoggerFactory.getLogger(BTreeNodeTracker.class);

    private final Set<Long> pendingDeleteNodes = new HashSet<>();
    private final Set<BTreeNode<E>> updatedNodes = new HashSet<>();

    public void markForUpdate(BTreeNode<E> node) {
        if (node.getNodeState() == BTreeNodeState.PERSISTED && node.getNodeId() != 0 && pendingDeleteNodes.contains(node.getNodeId())) {
            throw new IllegalArgumentException("Can't mark node for update when it's marked for removal. Node id " + node.getNodeId());
        }
        updatedNodes.add(node);

    }

    public void markForDelete(BTreeNode<E> node) {
        updatedNodes.remove(node);
        if (node.getNodeState() == BTreeNodeState.NEW || node.getNodeId() == 0) {
            return;
        }
        pendingDeleteNodes.add(node.getNodeId());
    }

    public void reset() {
        pendingDeleteNodes.clear();
        updatedNodes.clear();
    }

    public Set<Long> getPendingDeleteNodes() {
        return pendingDeleteNodes;
    }

    public Set<BTreeNode<E>> getUpdatedNodes() {
        return updatedNodes;
    }
}
