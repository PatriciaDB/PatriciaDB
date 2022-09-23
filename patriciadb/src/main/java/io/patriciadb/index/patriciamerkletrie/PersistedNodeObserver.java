package io.patriciadb.index.patriciamerkletrie;

import io.patriciadb.index.patriciamerkletrie.nodes.Node;

public interface PersistedNodeObserver {

    PersistedNodeObserver VOID_OBSERVER = new VoidPersistedNodeObserver();


    void newNodePersisted(Node node);

    void persistedNodeLostReference(Node node);

    class VoidPersistedNodeObserver implements PersistedNodeObserver {
        @Override
        public void newNodePersisted(Node node) {

        }

        @Override
        public void persistedNodeLostReference(Node node) {

        }
    }

}
