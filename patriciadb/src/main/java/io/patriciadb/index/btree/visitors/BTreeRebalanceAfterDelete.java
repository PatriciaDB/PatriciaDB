package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.BTreeContext;
import io.patriciadb.index.btree.io.BTreeNodeLoader;
import io.patriciadb.index.btree.io.BTreeNodeTracker;
import io.patriciadb.index.btree.nodes.BTreeNode;
import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;

public class BTreeRebalanceAfterDelete<E extends Comparable<E>> {
    private final BTreeContext context;
    private final BTreeNodeLoader<E> nodeLoader;
    private final BTreeNodeTracker<E> nodeTracker;

    public BTreeRebalanceAfterDelete(BTreeContext context, BTreeNodeLoader<E> nodeLoader, BTreeNodeTracker<E> nodeTracker) {
        this.context = context;
        this.nodeLoader = nodeLoader;
        this.nodeTracker = nodeTracker;
    }

    public E popLastFromLeaf(BTreeNode<E> node) {
        return node.apply(new PopLastFromLeaf());
    }

    public E popFirstFromLeaf(BTreeNode<E> node) {
        return node.apply(new PopFirstFromLeaf());
    }


    public void balanceChild(int position, InternalNode<E> internalNode, Object childRef) {
        var childNode = nodeLoader.loadNode(childRef);
        if (childNode.entrySize() >= context.getM_Half()) {
            return;
        }
        nodeTracker.markForUpdate(internalNode);
        if (childNode instanceof LeafNode<E> leafNode) {
            var leftSibling = internalNode.getLeftSiblingIfAvailable(position);
            var rightSibling = internalNode.getRightSiblingIfAvailable(position);

            LeafNode<E> leftChild = null;
            LeafNode<E> rightChild = null;
            if (leftSibling != null) {
                leftChild = (LeafNode<E>) nodeLoader.loadNode(leftSibling);
                if (leftChild.entrySize() > context.getM_Half()) { // left rotation
                    var entry = popLastFromLeaf(leftChild);
                    var last = internalNode.replaceEntry(position - 1, entry);
                    leafNode.prepend(last);
                    nodeTracker.markForUpdate(leftChild);
                    nodeTracker.markForUpdate(leafNode);
                    return;
                }
            }
            if (rightSibling != null) { // right rotation
                rightChild = (LeafNode<E>) nodeLoader.loadNode(rightSibling);
                if (rightChild.entrySize() > context.getM_Half()) {
                    var entry = popFirstFromLeaf(rightChild);
                    var last = internalNode.replaceEntry(position + 1, entry);
                    leafNode.append(last);
                    nodeTracker.markForUpdate(rightChild);
                    nodeTracker.markForUpdate(leafNode);
                    return;
                }
            }

            if (leftChild != null) { // Merge LeftLeafNode with LeafNode
                var entry = internalNode.getEntry(position - 1);
                leftChild.append(entry).appendLeaf(leafNode);
                internalNode.removeEntry(position - 1, leftChild);
                nodeTracker.markForUpdate(leftChild);
                nodeTracker.markForDelete(leafNode);
                return;
            } else if (rightChild != null) { // Merge LeftLeafNode with LeafNode
                var entry = internalNode.getEntry(position + 1);
                leafNode.append(entry).appendLeaf(rightChild);
                internalNode.removeEntry(position + 1, leafNode);
                nodeTracker.markForUpdate(leafNode);
                nodeTracker.markForDelete(rightChild);
                return;
            } else {
                throw new IllegalStateException("Unreachable state");
            }

        } else if (childNode instanceof InternalNode<E> branch) {
            var leftSibling = internalNode.getLeftSiblingIfAvailable(position);
            var rightSibling = internalNode.getRightSiblingIfAvailable(position);

            InternalNode<E> leftChild = null;
            InternalNode<E> rightChild = null;
            if (leftSibling != null) {
                leftChild = (InternalNode<E>) nodeLoader.loadNode(leftSibling);
                if (leftChild.entrySize() > context.getM_Half()) { // left rotation
                    var entry = internalNode.getEntry(position - 1);
                    var entryToReplace = InternalNode.shiftRight(leftChild, entry, branch);
                    internalNode.replaceEntry(position - 1, entryToReplace);
                    nodeTracker.markForUpdate(leftChild);
                    nodeTracker.markForUpdate(branch);
                    return;
                }
            }
            if (rightSibling != null) { // right rotation
                rightChild = (InternalNode<E>) nodeLoader.loadNode(rightSibling);
                if (rightChild.entrySize() > context.getM_Half()) {
                    var entry = internalNode.getEntry(position + 1);
                    var entryToReplace = InternalNode.shiftLeft(branch, entry, rightChild);
                    internalNode.replaceEntry(position + 1, entryToReplace);
                    nodeTracker.markForUpdate(rightChild);
                    nodeTracker.markForUpdate(branch);
                    return;
                }
            }

            if (leftChild != null) { // Merge LeftNode with Branch
                var entry = internalNode.getEntry(position - 1);
                leftChild.merge(entry, branch);
                internalNode.removeEntry(position - 1, leftChild);
                nodeTracker.markForUpdate(leftChild);
                nodeTracker.markForDelete(branch);
                return;
            } else if (rightChild != null) { // Merge RightNode with Branch

                var entry = internalNode.getEntry(position + 1);
                branch.merge(entry, rightChild);
                internalNode.removeEntry(position + 1, branch);
                nodeTracker.markForUpdate(branch);
                nodeTracker.markForDelete(rightChild);
                return;
            } else {
                throw new IllegalStateException("Unreachable state");
            }

        }

        throw new IllegalStateException("Unreachable state");

    }

    private class PopLastFromLeaf implements BTreeVisitor<E, E> {
        @Override
        public E apply(InternalNode<E> internalNode) {
            var p = internalNode.size() - 1;
            var lastNode = nodeLoader.loadNode(internalNode.getChild(p));
            var entry = lastNode.apply(this);
            balanceChild(p, internalNode, lastNode);
            return entry;
        }

        @Override
        public E apply(LeafNode<E> leafNode) {
            var entry = leafNode.removeLast();
            nodeTracker.markForUpdate(leafNode);
            return entry;
        }
    }

    private class PopFirstFromLeaf implements BTreeVisitor<E, E> {

        @Override
        public E apply(InternalNode<E> internalNode) {
            var lastNode = nodeLoader.loadNode(internalNode.getChild(0));
            var entry = lastNode.apply(this);
            balanceChild(0, internalNode, lastNode);
            return entry;
        }

        @Override
        public E apply(LeafNode<E> leafNode) {
            var entry = leafNode.removeFirst();
            nodeTracker.markForUpdate(leafNode);
            return entry;
        }
    }
}
