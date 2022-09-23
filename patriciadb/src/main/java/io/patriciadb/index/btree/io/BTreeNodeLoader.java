package io.patriciadb.index.btree.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.BTreeNodeState;
import io.patriciadb.index.btree.serialize.BTreeNodeDeserializer;

import java.util.OptionalLong;

public class BTreeNodeLoader<E extends Comparable<E>> {

    private final BlockReader storageReader;
    private final BTreeNodeDeserializer<E>  nodeDeserializer;
    // We need to load a node and keep it in a Weak or SoftReference, otherwise
    // we can load an old version of a modified node causing massive damage.
    // If we don't want to use a cache, the NodeLoader has to work in collaboration with
    // the NodeTracker to return any Node that has been modified
    private final LoadingCache<Long, BTreeNode<E>> nodeCache = Caffeine.newBuilder()
            .weakValues()
            .build(this::loadNodeFromFs);

    public BTreeNodeLoader(BlockReader storageReader, BTreeNodeDeserializer<E> nodeDeserializer) {
        this.storageReader = storageReader;
        this.nodeDeserializer = nodeDeserializer;
    }

    @SuppressWarnings("unchecked")
    public BTreeNode<E> loadNode(Object ref) {
        if(ref instanceof BTreeNode node) {
            return (BTreeNode<E>) node;
        }
        if(ref instanceof Long nodeId) {
            return loadNode(nodeId.longValue());
        }
        throw new IllegalArgumentException("Invalid node Ref "+ref);
    }

    public OptionalLong getNodeId(Object ref) {
        if(ref instanceof BTreeNode node) {
            return node.getNodeId()!=0 ? OptionalLong.of(node.getNodeId()) : OptionalLong.empty();
        }
        if(ref instanceof Long nodeId) {
            return OptionalLong.of(nodeId);
        }
        throw new IllegalArgumentException("Invalid node Ref "+ref);
    }

    public BTreeNode<E> loadNode(long nodeId) {
        return nodeCache.get(nodeId);
    }

    private BTreeNode<E> loadNodeFromFs(long nodeId) {
        var buffer = storageReader.read(nodeId);
        if(buffer==null) {
            throw new IllegalArgumentException("Node "+nodeId+" not found");
        }
        var node= nodeDeserializer.deserialise(buffer);
        node.setNodeId(nodeId);
        node.setNodeState(BTreeNodeState.PERSISTED);
        return node;
    }
}
