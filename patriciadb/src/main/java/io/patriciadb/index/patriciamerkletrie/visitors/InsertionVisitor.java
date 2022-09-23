package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.nodes.*;
import io.patriciadb.index.patriciamerkletrie.utils.Nibble;
import io.patriciadb.index.patriciamerkletrie.PersistedNodeObserver;

import java.util.Arrays;

public class InsertionVisitor implements PathNodeVisitor<InsertionVisitor.InsertionResult> {
    private final NodeLoader nodeLoader;
    private final PersistedNodeObserver persistedNodeObserver;
    private final byte[] value;

    public InsertionVisitor(NodeLoader nodeLoader, byte[] value, PersistedNodeObserver persistedNodeObserver) {
        this.nodeLoader = nodeLoader;
        this.value = value;
        this.persistedNodeObserver = persistedNodeObserver;
    }

    @Override
    public InsertionResult apply(final BranchNode branch, Nibble nibble) {
        if (nibble.isEmpty()) {
            var branchValueNode = branch.getValue();
            if (branchValueNode != null) {
                var valueNode = (LeafNode) nodeLoader.loadNode(branchValueNode);
                if (Arrays.equals(valueNode.getValue(), value)) {
                    return new InsertionResult(branch, false);
                }
            }
            if (persistedNodeObserver != null && branch.getState() == NodeState.PERSISTED) {
                persistedNodeObserver.persistedNodeLostReference(branch);
            }
            return new InsertionResult(branch.replaceValue(new LeafNode(value)), true);
        }
        var bucket = nibble.get(0);
        var child = branch.getChild(bucket);
        var subNibble = nibble.removePrefix(1);

        if (child != null) {
            var newNode = nodeLoader.loadNode(child).apply(this, subNibble);
            if (!newNode.subtreeChanged()) {
                return new InsertionResult(branch, false);
            }
            if (persistedNodeObserver != null && branch.getState() == NodeState.PERSISTED) {
                persistedNodeObserver.persistedNodeLostReference(branch);
            }
            var newBranch = branch.replaceChild(bucket, newNode.subChild);
            return new InsertionResult(newBranch, true);
        } else {
            Node newChild = new LeafNode(value);
            if (!subNibble.isEmpty()) {
                newChild = new ExtensionNode(subNibble, newChild);
            }
            var newBranch = branch.replaceChild(bucket, newChild);
            if (persistedNodeObserver != null && branch.getState() == NodeState.PERSISTED) {
                persistedNodeObserver.persistedNodeLostReference(branch);
            }
            return new InsertionResult(newBranch, true);
        }
    }

    @Override
    public InsertionResult apply(ExtensionNode extension, Nibble nibble) {
        var extNibble = extension.getNibble();
        if (nibble.isEmpty()) {
            var branchNode = BranchNode.createNew().replaceValue(new LeafNode(value));
            int bucket = extNibble.get(0);
            Object newChild = extension.removeFirstBucket();
            branchNode = branchNode.replaceChild(bucket, newChild);
            if (persistedNodeObserver != null && extension.getState() == NodeState.PERSISTED) {
                persistedNodeObserver.persistedNodeLostReference(extension);
            }
            return new InsertionResult(branchNode, true);
        }
        if (nibble.startWith(extNibble)) {
            var newNibble = nibble.removePrefix(extNibble.size());
            var insertOperation = nodeLoader.loadNode(extension.getNextNode()).apply(this, newNibble);
            if (!insertOperation.subtreeChanged()) {
                return new InsertionResult(extension, false);
            }
            if (persistedNodeObserver != null && extension.getState() == NodeState.PERSISTED) {
                persistedNodeObserver.persistedNodeLostReference(extension);
            }
            var newChild = new ExtensionNode(extNibble, insertOperation.subChild);
            return new InsertionResult(newChild, true);

        }
        var parts = Nibble.extractPrefix(nibble, extNibble);
        var common = parts[0];
        var left = parts[1];
        var right = parts[2]; // the right one is never empty
        if (right.isEmpty()) {
            throw new IllegalStateException();
        }
        var branch = BranchNode.createNew();
        if (left.isEmpty()) {
            branch = branch.replaceValue(new LeafNode(value));
        } else {
            branch = branch.insert(left, new LeafNode(value));
        }
        branch = branch.insert(right, extension.getNextNode());

        var child = common.isEmpty() ? branch : new ExtensionNode(common, branch);
        if (persistedNodeObserver != null && extension.getState() == NodeState.PERSISTED) {
            persistedNodeObserver.persistedNodeLostReference(extension);
        }
        return new InsertionResult(child, true);
    }

    @Override
    public InsertionResult apply(LeafNode leaf, Nibble nibble) {
        if (nibble.isEmpty()) {
            if (Arrays.equals(leaf.getValue(), value)) {
                return new InsertionResult(leaf, false);
            } else {
                if (persistedNodeObserver != null && leaf.getState() == NodeState.PERSISTED) {
                    persistedNodeObserver.persistedNodeLostReference(leaf);
                }
                return new InsertionResult(new LeafNode(value), true);
            }
        }
        var branch = BranchNode.createNew()
                .insert(nibble, new LeafNode(value))
                .replaceValue(leaf);
        return new InsertionResult(branch, true);
    }

    @Override
    public InsertionResult apply(EmptyNode empty, Nibble nibble) {
        if (persistedNodeObserver != null && empty.getState() == NodeState.PERSISTED) {
            persistedNodeObserver.persistedNodeLostReference(empty);
        }
        var leaf = new LeafNode(value);
        if (nibble.isEmpty()) {
            return new InsertionResult(leaf, true);
        }
        var ext = new ExtensionNode(nibble, leaf);
        return new InsertionResult(ext, true);
    }


    public record InsertionResult(Node subChild, boolean subtreeChanged) {

    }
}
