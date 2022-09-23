package io.patriciadb.index.patriciamerkletrie.utils;

import io.patriciadb.index.patriciamerkletrie.PersistedNodeObserver;
import io.patriciadb.index.patriciamerkletrie.nodes.Node;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class PersistedNodeObserverTracker implements PersistedNodeObserver {
    private final Roaring64NavigableMap lostNodes;
    private final Roaring64NavigableMap newNodes;

    public PersistedNodeObserverTracker() {
        lostNodes = new Roaring64NavigableMap(false);
        newNodes = new Roaring64NavigableMap(false);
    }

    @Override
    public void newNodePersisted(Node node) {
        if (node.getNodeId() == 0) {
            throw new IllegalArgumentException("Invalid node id 0");
        }
        newNodes.addLong(node.getNodeId());
    }

    @Override
    public void persistedNodeLostReference(Node node) {
        if (node.getNodeId() == 0) {
            throw new IllegalArgumentException("Invalid node id 0");
        }
        lostNodes.addLong(node.getNodeId());
    }

    public Roaring64NavigableMap getLostNodes() {
        return lostNodes;
    }

    public Roaring64NavigableMap getNewNodes() {
        return newNodes;
    }
}
