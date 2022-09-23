package io.patriciadb.index.btree.visitors;

import io.patriciadb.index.btree.BTreeContext;
import io.patriciadb.index.btree.KeyRange;
import io.patriciadb.index.btree.io.BTreeNodeLoader;
import io.patriciadb.index.btree.nodes.InternalNode;
import io.patriciadb.index.btree.nodes.LeafNode;

import java.util.ArrayList;
import java.util.List;

public class BTreeFindRangeEntryVisitor<E extends Comparable<E>> implements BTreeVisitorParam<E, List<E>, Void> {

    private final BTreeContext context;
    private final BTreeNodeLoader<E> nodeLoader;
    private final KeyRange<E> keyRange;

    public BTreeFindRangeEntryVisitor(BTreeContext context, BTreeNodeLoader<E> nodeLoader, KeyRange<E> keyRange) {
        this.context = context;
        this.nodeLoader = nodeLoader;
        this.keyRange = keyRange;
    }

    public List<E> find(Object nodeRef) {
        var result = new ArrayList<E>();
        nodeLoader.loadNode(nodeRef).apply(this, result);
        return result;
    }

    @Override
    public Void apply(InternalNode<E> internalNode, List<E> result) {
        int p = internalNode.positionOf(keyRange.getFromEntry());
        while (p < internalNode.size()) {
            if (internalNode.isChild(p)) {
                var subnode = nodeLoader.loadNode(internalNode.getChild(p));
                subnode.apply(this, result);
            } else {
                var entry = internalNode.getEntry(p);
                if (keyRange.getFromEntry().compareTo(entry) > 0 || keyRange.getToEntry().compareTo(entry) < 0) {
                    return null;
                }
                result.add(entry);
            }
            p++;
        }
        return null;
    }

    @Override
    public Void apply(LeafNode<E> leafNode, List<E> result) {
        int p = leafNode.findFirst(keyRange.getFromEntry());
        for (int i = p; i < leafNode.size(); i++) {
            var entry = leafNode.getEntry(i);
            if (keyRange.getFromEntry().compareTo(entry) > 0 || keyRange.getToEntry().compareTo(entry) < 0) {
                return null;
            }
            result.add(entry);

        }
        return null;
    }
}
