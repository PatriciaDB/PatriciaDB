package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.BTreeContext;
import io.patriciadb.index.btree.io.BTreeNodeLoader;
import io.patriciadb.index.btree.io.BTreeNodeTracker;
import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BTreeDeleteVisitor<E extends Comparable<E>>
        implements BTreeVisitorParam<E, E, BTreeDeleteVisitor.DeleteResult<E>> {
    private final static Logger log = LoggerFactory.getLogger(BTreeDeleteVisitor.class);
    private final BTreeContext context;
    private final BTreeNodeLoader<E> nodeLoader;
    private final BTreeNodeTracker<E> nodeTracker;
    private final BTreeRebalanceAfterDelete<E> rebalanceAfterDelete;

    public BTreeDeleteVisitor(BTreeContext context,
                              BTreeNodeLoader<E> nodeLoader,
                              BTreeNodeTracker<E> nodeTracker,
                              BTreeRebalanceAfterDelete<E> rebalanceAfterDelete) {
        this.context = context;
        this.nodeLoader = nodeLoader;
        this.nodeTracker = nodeTracker;
        this.rebalanceAfterDelete = rebalanceAfterDelete;
    }

    public DeleteResult<E> deleteEntry(Object rootNode, E entry) {
        var deleteResult = nodeLoader.loadNode(rootNode).apply(this, entry);
        if(!deleteResult.modified()) {
            return new DeleteResult<>(deleteResult.subNode(), false);
        }
        if(deleteResult.subNode().entrySize()==0 && deleteResult.subNode() instanceof InternalNode<E> internalNode) { // reduce height
            var newRoot = nodeLoader.loadNode(internalNode.getFirstChild());
            nodeTracker.markForDelete(deleteResult.subNode());
            return new DeleteResult<>(newRoot, true);
        }
        return new DeleteResult<>(deleteResult.subNode(), true);
    }

    @Override
    public DeleteResult<E> apply(InternalNode<E> internalNode, E entry) {
        int p = internalNode.positionOf(entry);
        if (internalNode.isChild(p)) {
            var node = nodeLoader.loadNode(internalNode.getChild(p));
            var  deleteResult = node.apply(this, entry);
            if (!deleteResult.modified()) {
                return new DeleteResult<>(internalNode, false);
            }
            internalNode.replaceChild(p, deleteResult.subNode());
            rebalanceAfterDelete.balanceChild(p, internalNode, node);
            return new DeleteResult<>(internalNode, true);
        }
        if(internalNode.isEntry(p)) {
            var leftChild = internalNode.getChild(p-1);
            var leftChildNode = nodeLoader.loadNode(leftChild);
            var predecessorEntry = rebalanceAfterDelete.popLastFromLeaf(leftChildNode);
            internalNode.replaceEntry(p, predecessorEntry);
            rebalanceAfterDelete.balanceChild(p-1, internalNode, leftChildNode);
            nodeTracker.markForUpdate(internalNode);
            return new DeleteResult<>(internalNode, true);
        }
        throw new IllegalStateException("Unreachable state");
    }

    @Override
    public DeleteResult<E> apply(LeafNode<E> leafNode, E param) {
        int p =leafNode.find(param);
        if(p<0) return new DeleteResult<>(leafNode, false);
        leafNode.remove(p);
        nodeTracker.markForUpdate(leafNode);
        return new DeleteResult<>(leafNode, true);
    }

    public record DeleteResult<E extends Comparable<E>>(BTreeNode<E> subNode, boolean modified) {

    }
}
