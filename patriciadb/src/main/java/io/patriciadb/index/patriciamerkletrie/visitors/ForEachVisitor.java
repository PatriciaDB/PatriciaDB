package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;
import io.patriciadb.index.patriciamerkletrie.utils.Nibble;

import java.util.function.BiConsumer;

public class ForEachVisitor implements PathNodeVisitor<Void> {

    private final NodeLoader nodeLoader;
    private final BiConsumer<byte[], byte[]> consumer;

    public ForEachVisitor(NodeLoader nodeLoader, BiConsumer<byte[], byte[]> consumer) {
        this.nodeLoader = nodeLoader;
        this.consumer = consumer;
    }

    @Override
    public Void apply(BranchNode branch, Nibble nibble) {
        if(branch.getValue()!=null) {
            var valueChild = nodeLoader.loadNode(branch.getValue());
            valueChild.apply(this, nibble);
        }
        for(int i =0; i<16; i++) {
            var childRef = branch.getChild(i);
            if(childRef==null)  continue;

            var child = nodeLoader.loadNode(childRef);
            var childNibble = nibble.append(i);
            child.apply(this, childNibble);
        }
        return null;
    }

    @Override
    public Void apply(ExtensionNode extension, Nibble nibble) {
        var nextNode = nodeLoader.loadNode(extension.getNextNode());
        var newNibble = nibble.append(extension.getNibble());
        nextNode.apply(this, newNibble);
        return null;
    }

    @Override
    public Void apply(LeafNode leaf, Nibble nibble) {
        consumer.accept(nibble.pack(), leaf.getValue().clone());
        return null;
    }

    @Override
    public Void apply(EmptyNode empty, Nibble nibble) {
        return null;
    }
}
