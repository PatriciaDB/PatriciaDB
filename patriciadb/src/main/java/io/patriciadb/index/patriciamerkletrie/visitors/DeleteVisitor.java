package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.PersistedNodeObserver;
import io.patriciadb.index.patriciamerkletrie.nodes.*;
import io.patriciadb.index.patriciamerkletrie.utils.Nibble;

public class DeleteVisitor implements PathNodeVisitor<DeleteVisitor.DeleteResult> {
    private final NodeLoader nodeLoader;
    private final PersistedNodeObserver persistedNodeObserver;

    private DeleteVisitor(NodeLoader nodeLoader, PersistedNodeObserver persistedNodeObserver) {
        this.nodeLoader = nodeLoader;
        this.persistedNodeObserver = persistedNodeObserver;
    }

    public static Result delete(NodeLoader nodeLoader, Node rootNode, Nibble nibble, PersistedNodeObserver persistedNodeObserver) {
        var deleteVisitor = new DeleteVisitor(nodeLoader, persistedNodeObserver);
        var result = rootNode.apply(deleteVisitor, nibble);
        return new Result(result.modified(), result.subTree());
    }

    public record Result(boolean modified, Node newRootNode) {

    }


    @Override
    public DeleteResult apply(BranchNode branch, Nibble nibble) {

        if (nibble.isEmpty()) {
            if (branch.getValue()==null) {
                return new DeleteResult(branch, false);
            }
            var newNode = flatBranch(branch.clearValue());
            if(persistedNodeObserver != null && branch.getState() == NodeState.PERSISTED) {
                persistedNodeObserver.persistedNodeLostReference(branch);
            }
            return new DeleteResult(newNode, true);
        }
        var bucket = nibble.get(0);
        if (branch.getChild(bucket) == null) {
            return new DeleteResult(branch, false);
        }
        var newNibble = nibble.removePrefix(1);
        var deleteResult = nodeLoader.loadNode(branch.getChild(bucket)).apply(this, newNibble);
        if(!deleteResult.modified()) {
            return new DeleteResult(branch, false);
        }
        if(persistedNodeObserver != null && branch.getState() == NodeState.PERSISTED) {
            persistedNodeObserver.persistedNodeLostReference(branch);
        }
        if(deleteResult.subTree.isEmptyNode()) {
            var newNode = flatBranch(branch.clearChild(bucket));
            return new DeleteResult(newNode,true);
        }
        var newNode = flatBranch(branch.replaceChild(bucket,deleteResult.subTree));
        return new DeleteResult(newNode, true);
    }

    private Node flatBranch(BranchNode branch) {
        var normalisedBranch = branch.normaliseBranch();
        var subNode= nodeLoader.loadNode(normalisedBranch.right());
        var subNibble = normalisedBranch.left();
        if(subNibble.isEmpty()) {
            return subNode;
        }
        Node result;
        if(subNode instanceof ExtensionNode ext) {
            result = new ExtensionNode(subNibble.append(ext.getNibble()), ext.getNextNode());
        } else {
            result = new ExtensionNode(subNibble, subNode);
        }
        return result;
    }

    @Override
    public DeleteResult apply(ExtensionNode extension, Nibble nibble) {
        if (nibble.size() < extension.getNibble().size()) {
            return new DeleteResult(extension, false);
        }
        if (!nibble.startWith(extension.getNibble())) {
            return new DeleteResult(extension, false);
        }
        var newNibble = nibble.removePrefix(extension.getNibble().size());
        var deleteResult = nodeLoader.loadNode(extension.getNextNode()).apply(this, newNibble);
        if (!deleteResult.modified()) {
            return new DeleteResult(extension, false);
        }
        if(persistedNodeObserver != null && extension.getState() == NodeState.PERSISTED) {
            persistedNodeObserver.persistedNodeLostReference(extension);
        }
        if(deleteResult.subTree.isEmptyNode()) {
            return new DeleteResult(EmptyNode.INSTANCE, true);
        }
        Node nodeResult;
        if (deleteResult.subTree() instanceof ExtensionNode ext) {
            nodeResult = new ExtensionNode(extension.getNibble().append(ext.getNibble()), ext.getNextNode());
        } else {
            nodeResult = new ExtensionNode(extension.getNibble(), deleteResult.subTree());
        }
        return new DeleteResult(nodeResult, true);
    }

    @Override
    public DeleteResult apply(LeafNode leaf, Nibble nibble) {
        if (!nibble.isEmpty()) {
            return new DeleteResult(leaf, false);
        }
        if(persistedNodeObserver != null && leaf.getState() == NodeState.PERSISTED) {
            persistedNodeObserver.persistedNodeLostReference(leaf);
        }
        return new DeleteResult(EmptyNode.INSTANCE, true);
    }

    @Override
    public DeleteResult apply(EmptyNode empty, Nibble nibble) {
        return new DeleteResult(empty, false);
    }

    public record DeleteResult(Node subTree, boolean modified){}


}
