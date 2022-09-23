package io.patriciadb.index.btree;

import io.patriciadb.fs.BlockReader;
import io.patriciadb.fs.BlockWriter;
import io.patriciadb.index.btree.io.BTreeNodeLoader;
import io.patriciadb.index.btree.io.BTreeNodeTracker;
import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.LeafNode;
import io.patriciadb.index.btree.serialize.BTreeNodeDeserializer;
import io.patriciadb.index.btree.serialize.BTreePersistChanges;
import io.patriciadb.index.btree.visitors.BTreeDeleteVisitor;
import io.patriciadb.index.btree.visitors.BTreeFindRangeEntryVisitor;
import io.patriciadb.index.btree.visitors.BTreeInsertVisitor;
import io.patriciadb.index.btree.visitors.BTreeRebalanceAfterDelete;
import io.patriciadb.utils.Serializer;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

public class BTree<E extends Comparable<E>> {

    private Object rootNode;
    private final BTreeContext context = new BTreeContext();
    private final BTreeNodeLoader<E> nodeLoader;
    private final BTreeNodeTracker<E> nodeTracker = new BTreeNodeTracker<>();
    private final BTreeInsertVisitor<E> insertVisitor;
    private final BTreeDeleteVisitor<E> deleter;
    private final Serializer<E> entrySerializer;

    @SuppressWarnings("unchecked")
    private BTree(Object rootNodeRef,
                  Serializer<E> entrySerializer,
                  BlockReader storageReader) {
        this.entrySerializer = Objects.requireNonNull(entrySerializer);
        var nodeDeserializer = new BTreeNodeDeserializer<>(entrySerializer);
        nodeLoader = new BTreeNodeLoader<>(storageReader, nodeDeserializer);
        insertVisitor = new BTreeInsertVisitor<>(context, nodeLoader, nodeTracker);
        var rebalanceAfterDelete = new BTreeRebalanceAfterDelete<>(context, nodeLoader, nodeTracker);
        deleter = new BTreeDeleteVisitor<>(context,nodeLoader, nodeTracker, rebalanceAfterDelete);
        if (rootNodeRef instanceof BTreeNode<?> node) { // New node
            this.rootNode = node;
            nodeTracker.markForUpdate((BTreeNode<E>) node);
        } else if(rootNodeRef instanceof Long rootId) {
            this.rootNode = rootId;
        } else {
            throw new IllegalArgumentException("Root not "+rootNodeRef+" not valid");
        }

    }

    public static <E extends Comparable<E>> BTree<E> createNew(Serializer<E> entrySerializer) {
        return new BTree<>(new LeafNode<E>(List.of()), entrySerializer, nodeId -> null);
    }

    public static <E extends Comparable<E>> BTree<E> open(long rootId,
                                                          Serializer<E> entrySerializer,
                                                          BlockReader storageReader) {
        return new BTree<>(rootId, entrySerializer, storageReader);
    }

    public static <E extends Comparable<E>> BTree<E> openOrCreate(long rootId,
                                                          Serializer<E> entrySerializer,
                                                          BlockReader storageReader) {
        if(rootId==0) {
            return BTree.createNew(entrySerializer);
        } else {
            return BTree.open(rootId, entrySerializer, storageReader);
        }
    }


    public void persistChanges(BlockWriter storage) {
        var persister = new BTreePersistChanges<>(storage, entrySerializer);
        persister.persist(nodeTracker);
    }

    public OptionalLong getRootId() {
        return nodeLoader.getNodeId(rootNode);
    }

    public void insert(E entry) {
        rootNode = insertVisitor.put(rootNode, entry).node();
    }

    public boolean delete(E entry) {
        var result = deleter.deleteEntry(rootNode, entry);
        if (!result.modified()) {
            return false;
        }
        rootNode = result.subNode();
        return true;
    }

    @SuppressWarnings("unchecked")
    public List<E> find(KeyRange<E> keyRange) {
        var rangeFinder = new BTreeFindRangeEntryVisitor<>(context, nodeLoader, keyRange);
        return rangeFinder.find(rootNode);
    }
}
