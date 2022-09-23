package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.BTreeContext;
import io.patriciadb.index.btree.io.BTreeNodeTracker;
import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;

import java.util.Optional;

public class NodeSplitterVisitor<E extends Comparable<E>>
        implements BTreeVisitor<E, NodeSplitterVisitor.NodeSplitResult<E>>{
    private final BTreeContext context;
    private final BTreeNodeTracker<E> nodeTracker;

    public NodeSplitterVisitor(BTreeContext context, BTreeNodeTracker<E> nodeTracker) {
        this.context = context;
        this.nodeTracker = nodeTracker;
    }

    public Optional<NodeSplitResult<E>> split(BTreeNode<E> node) {
        return Optional.ofNullable(node.apply(this));
    }

    @Override
    public NodeSplitResult<E> apply(InternalNode<E> internalNode) {
        if (internalNode.size() < 7) {
            return null;
        }
        int p = (internalNode.size() - 1) / 2;
        if (internalNode.isChild(p)) p++;
        E entry = internalNode.getEntry(p);
        var left = new InternalNode<E>(internalNode.subList(0, p), internalNode.getDepth());
        var right = new InternalNode<E>(internalNode.subList(p + 1, internalNode.size()), internalNode.getDepth());
        nodeTracker.markForDelete(internalNode);
        nodeTracker.markForUpdate(left);
        nodeTracker.markForUpdate(right);

        return new NodeSplitResult<>(left, entry, right);
    }

    @Override
    public NodeSplitResult<E> apply(LeafNode<E> leafNode) {
        if(leafNode.size()<3) {
            return null;
        }
        int center = leafNode.size() / 2;
        var centerValue = leafNode.getEntry(center);
        var leftList = leafNode.subList(0, center);
        var rightRight = leafNode.subList(center + 1, leafNode.size());
        var leftNode = new LeafNode<>(leftList);
        var rightNode = new LeafNode<>(rightRight);
        nodeTracker.markForDelete(leafNode);
        nodeTracker.markForUpdate(leftNode);
        nodeTracker.markForUpdate(rightNode);
        return new NodeSplitResult<>(leftNode,centerValue, rightNode);
    }

    public record NodeSplitResult<E extends Comparable<E>>(BTreeNode<E> left, E center, BTreeNode<E> right) {

    }
}
