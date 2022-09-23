package io.patriciadb.index.btree.nodes;

import io.patriciadb.index.btree.visitors.BTreeVisitorParam;
import io.patriciadb.index.btree.visitors.BTreeVisitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LeafNode<E extends Comparable<E>> extends BTreeNode<E>{
    private final ArrayList<E> entries;

    public LeafNode(ArrayList<E> entries) {
        this.entries = entries;
    }
    public LeafNode(Collection<E> entries) {
        this(new ArrayList<>(entries));
    }

    public LeafNode() {
        this.entries = new ArrayList<>();
    }


    @Override
    public <T> T apply(BTreeVisitor<E, T> visitor) {
        return visitor.apply(this);
    }


    @Override
    public <P, T> T apply(BTreeVisitorParam<E, P,T> visitor, P param) {
        return visitor.apply(this, param);
    }

    public int find(E entry) {
        return Collections.binarySearch(entries, entry);
    }

    public int findFirst(E key) {
        int p=Collections.binarySearch(entries, key);
        if(p>=0) {
            return p;
        }
        return (-p) - 1;
    }

    public void add(int position, E entry) {
        entries.add(position, entry);
    }

    public void replace(int position, E entry) {
        entries.set(position, entry);
    }

    public List<E> subList(int from, int to) {
        return entries.subList(from, to);
    }

    public int size() {
        return entries.size();
    }

    public E getEntry(int p) {
        return entries.get(p);
    }

    public E remove(int p) {
        return entries.remove(p);
    }

    @Override
    public int entrySize() {
        return entries.size();
    }

    public LeafNode<E> prepend(E entry) {
        entries.add(0, entry);
        return this;
    }

    public LeafNode<E> append(E entry) {
        entries.add(entry);
        return this;

    }
    public LeafNode<E> appendLeaf(LeafNode<E> toAppend) {
        entries.addAll(toAppend.entries);
        return this;
    }

    @Override
    public String toString() {
        return "LeafNode{" +
                "entries=" + entries +
                '}';
    }

    public E removeLast() {
        return entries.remove(entries.size()-1);
    }

    public E removeFirst() {
        return entries.remove(0);
    }

    @Override
    public int getDepth() {
        return 0;
    }
}
