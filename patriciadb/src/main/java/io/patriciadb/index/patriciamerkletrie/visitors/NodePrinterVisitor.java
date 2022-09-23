package io.patriciadb.index.patriciamerkletrie.visitors;

import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;
import org.apache.commons.lang3.StringUtils;

import java.util.HexFormat;

public class NodePrinterVisitor implements Visitor<Integer> {
    private final NodeLoader nodeLoader;
    private final String prefixChar;

    public NodePrinterVisitor(NodeLoader nodeLoader, String prefixChar) {
        this.nodeLoader = nodeLoader;
        this.prefixChar = prefixChar;
    }

    @Override
    public void apply(BranchNode branch, Integer depth) {
        var prefix = StringUtils.repeat(prefixChar, depth);
        System.out.println(prefix + "Branch {");
        for (int i = 0; i < 16; i++) {
            if (branch.getChild(i) != null) {
                var subNode = nodeLoader.loadNode(branch.getChild(i));
                subNode.apply(this, depth + 1);
            } else {
                System.out.println(prefix + ",");
            }
        }
        if (branch.getValue() != null) {
            var valueChild = nodeLoader.loadNode(branch.getValue());
            valueChild.apply(this, depth + 1);
        }
        System.out.println(prefix + "}");
    }

    @Override
    public void apply(ExtensionNode extension, Integer depth) {
        var prefix = StringUtils.repeat(prefixChar, depth);

        System.out.println(prefix + "Extension " + extension.getNibble() + " {");
        var nextNode = nodeLoader.loadNode(extension.getNextNode());
        nextNode.apply(this, depth + 1);
        System.out.println(prefix + "}");
    }

    @Override
    public void apply(LeafNode leaf, Integer depth) {
        var prefix = StringUtils.repeat(prefixChar, depth);
        System.out.println(prefix + "Value[" + HexFormat.of().formatHex(leaf.getValue()) + "]");
    }

    @Override
    public void apply(EmptyNode empty, Integer depth) {
        var prefix = StringUtils.repeat(prefixChar, depth);
        System.out.println(prefix + "NullNode");
    }
}
