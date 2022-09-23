package io.patriciadb.index.btree.nodes;

import io.patriciadb.index.btree.io.BTreeNodeLoader;
import io.patriciadb.index.btree.visitors.BTreeVisitorParam;
import io.patriciadb.index.btree.visitors.BTreeVisitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InternalNode<E extends Comparable<E>> extends BTreeNode<E> {

    private final int depth;
    private final List<Object> values;

    public InternalNode(Collection<Object> values, int depth) {
        this(new ArrayList<>(values), depth);
    }

    public InternalNode(ArrayList<Object> values, int depth) {
        if (values.size() % 2 == 0) {
            throw new IllegalArgumentException("Inner nodes must always have odd numbers of objects");
        }
        this.values = values;
        this.depth = depth;
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public <T> T apply(BTreeVisitor<E, T> visitor) {
        return visitor.apply(this);
    }

    @Override
    public <P, T> T apply(BTreeVisitorParam<E, P, T> visitor, P param) {
        return visitor.apply(this, param);
    }

    @SuppressWarnings("unchecked")
    public int positionOf(E entry) {
        if (values.size() == 1) {
            return 0;
        }
        for (int i = 1; i < values.size(); i += 2) {
            int c = ((E) values.get(i)).compareTo(entry);
            if (c == 0) {
                return i;
            }
            if (c > 0) {
                return i - 1;
            }
        }
        return values.size() - 1;
    }

    public void replaceChild(int p, BTreeNode<E> node) {
        if(!isChild(p)) throw new IllegalArgumentException(p+" is not a child");
        values.set(p, node);
    }

    public Object getFirstChild() {
        return values.get(0);
    }

    public boolean isChild(int p) {
        if (p < 0 || p >= values.size()) throw new IllegalArgumentException("Index out of range " + p);
        return p % 2 == 0;
    }

    public boolean isEntry(int p) {
        if (p < 0 || p >= values.size()) throw new IllegalArgumentException("Index out of range " + p);
        return p % 2 == 1;
    }

    @SuppressWarnings("unchecked")
    public E getEntry(int p) {
        if (!isEntry(p)) throw new IllegalArgumentException(p + " is not an entry index");
        return (E) values.get(p);
    }

    public Object getChild(int p) {
        if (!isChild(p)) throw new IllegalArgumentException(p + " is not a childNode index");
        return values.get(p);
    }

    @SuppressWarnings("unchecked")
    public BTreeNode<E> getChildForUpdate(int p, BTreeNodeLoader<E> nodeLoader) {
        var nodeRef = getChild(p);
        if (nodeRef instanceof BTreeNode<?> node) {
            return (BTreeNode<E>) node;
        } else if (nodeRef instanceof Long nodeId) {
            var node = nodeLoader.loadNode(nodeId.longValue());
            values.set(p, node);
            return node;
        } else {
            throw new IllegalStateException("Invalid child value, found " + nodeRef);
        }
    }

//    public void setChild(int p, Object child) {
//        if(!isChild(p)) throw new IllegalArgumentException(p+" is not a childNode index");
//        if(child instanceof BTreeNode<?> || child instanceof Long) {
//            values.set(p, child);
//        } else{
//            throw new IllegalArgumentException("Invalid child node "+child);
//        }
//
//    }

    public int size() {
        return values.size();
    }

    public List<Object> subList(int from, int to) {
        return values.subList(from, to);
    }

    public void replaceChildNode(int p, Object left, E entry, Object right) {
        if (isEntry(p)) throw new IllegalStateException("Element is not an child node");
        values.remove(p);
        values.add(p, right);
        values.add(p, entry);
        values.add(p, left);
    }

    public Object getLeftSiblingIfAvailable(int p) {
        if (isEntry(p)) throw new IllegalStateException("Element is not an child node");
        if (p - 2 >= 0) return values.get(p - 2);
        return null;
    }

    public Object getRightSiblingIfAvailable(int p) {
        if (isEntry(p)) throw new IllegalStateException("Element is not an child node");
        if (p + 2 < values.size()) return values.get(p + 2);
        return null;
    }

    @SuppressWarnings("unchecked")
    public E replaceEntry(int p, E entry) {
        if (isChild(p)) throw new IllegalStateException("Element is not an Entry");
        return (E) values.set(p, entry);
    }

    public void removeEntry(int entryPosition, Object nodeReference) {
        if (isChild(entryPosition)) throw new IllegalStateException("Element is not an child node");
        values.remove(entryPosition - 1);
        values.remove(entryPosition - 1);
        values.remove(entryPosition - 1);
        values.add(entryPosition - 1, nodeReference);
    }

    public void merge(E centerEntry, InternalNode<E> rightNode) {
        values.add(centerEntry);
        values.addAll(rightNode.values);
    }

    @SuppressWarnings("unchecked")
    public static <E extends Comparable<E>> E shiftLeft(InternalNode<E> leftList, E center, InternalNode<E> rightList) {
        leftList.values.add(center);
        leftList.values.add(rightList.values.remove(0));
        return (E) rightList.values.remove(0);
    }

    @SuppressWarnings("unchecked")
    public static <E extends Comparable<E>> E shiftRight(InternalNode<E> leftList, E center, InternalNode<E> rightList) {
        var pointer = leftList.values.remove(leftList.values.size() - 1);
        var entry = (E) leftList.values.remove(leftList.values.size() - 1);
        rightList.values.add(0, center);
        rightList.values.add(0, pointer);
        return entry;
    }

    @Override
    public String toString() {
        return "InternalNode{" +
                "depth=" + depth +
                ", values=" + values +
                '}';
    }

    @Override
    public int entrySize() {
        return (values.size() - 1) / 2;
    }
}
