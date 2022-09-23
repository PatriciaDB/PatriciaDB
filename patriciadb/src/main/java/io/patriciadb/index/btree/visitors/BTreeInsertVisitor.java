package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.BTreeContext;
import io.patriciadb.index.btree.io.BTreeNodeLoader;
import io.patriciadb.index.btree.io.BTreeNodeTracker;
import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;
import io.patriciadb.index.btree.nodes.BTreeNode;

import java.util.List;

public class BTreeInsertVisitor<E extends Comparable<E>> implements BTreeVisitorParam<E, E, BTreeInsertVisitor.InsertResult<E>> {
    private final BTreeNodeLoader<E> nodeLoader;
    private final BTreeNodeTracker<E> nodeTracker;
    private final BTreeContext context;
    private final NodeSplitterVisitor<E> nodeSplitterVisitor;

    public BTreeInsertVisitor(BTreeContext context,
                              BTreeNodeLoader<E> nodeLoader,
                              BTreeNodeTracker<E> nodeTracker) {
        this.nodeLoader = nodeLoader;
        this.nodeTracker = nodeTracker;
        this.context = context;
        nodeSplitterVisitor = new NodeSplitterVisitor<>(context, nodeTracker);
    }

    public InsertResult<E> put(Object rootNodeRef, E entry) {
        var root = nodeLoader.loadNode(rootNodeRef);
        var insertResult = root.apply(this, entry);
        if(!insertResult.entryInserted()) {
            return new InsertResult<>(root, false);
        }
        if(insertResult.node().entrySize()>= context.getM()) {
            var splitResultOpt = nodeSplitterVisitor.split(insertResult.node());
            if(splitResultOpt.isPresent()) {
                var splitRes = splitResultOpt.get();
                var childEntries = List.of(splitRes.left(), splitRes.center(), splitRes.right());
                var newDepth = insertResult.node().getDepth()+1;
                var newRootNode = new InternalNode<E>(childEntries, newDepth);
                nodeTracker.markForUpdate(newRootNode);
                return new InsertResult<>(newRootNode, true);
            }
        }
        return new InsertResult<>(root, true);
    }

    @Override
    public InsertResult<E> apply(InternalNode<E> internalNode, E entry) {
        int p = internalNode.positionOf(entry);
        if (internalNode.isEntry(p)) {
            return new InsertResult<>(internalNode, false);
        }
        var subnode = nodeLoader.loadNode(internalNode.getChild(p));
        var childInsertionResult = subnode.apply(this, entry);
        if (!childInsertionResult.entryInserted()) {
            return new InsertResult<>(internalNode, false);
        }
        var newNode = childInsertionResult.node();
        if (newNode.entrySize() >= context.getM()) {
            var splitResultOpt = nodeSplitterVisitor.split(newNode);
            if(splitResultOpt.isPresent()) {
                var splitResult = splitResultOpt.get();
                internalNode.replaceChildNode(p, splitResult.left(), splitResult.center(), splitResult.right());
                nodeTracker.markForUpdate(internalNode);
            }
        }
        return new InsertResult<>(internalNode, true);
    }

    @Override
    public InsertResult<E> apply(LeafNode<E> leafNode, E entry) {
        int p = leafNode.find(entry);
        if (p >= 0) {
            return new InsertResult<>(leafNode, false);
        }
        leafNode.add((-p) - 1, entry);
        nodeTracker.markForUpdate(leafNode);
        return new InsertResult<>(leafNode, true);
    }

    public record InsertResult<E extends Comparable<E>>(BTreeNode<E> node, boolean entryInserted) {

    }


}
