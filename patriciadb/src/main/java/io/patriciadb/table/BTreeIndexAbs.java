package io.patriciadb.table;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.index.btree.BTree;
import io.patriciadb.index.btree.KeyRange;

public abstract class BTreeIndexAbs<E extends Entity, I extends Comparable<I>> implements Index<E> {
    protected final BTree<I> bTree;

    public BTreeIndexAbs(BTree<I> bTree) {
        this.bTree = bTree;
    }


    public void beforeInsert(E entry) {
        if (!isUnique()) {
            return;
        }
        var key = getSearchKey(entry);
        if (!bTree.find(key).isEmpty()) {
            throw new UniqueConstrainViolation("Entry " + entry + " violates unique constrain");
        }
    }

    public void insert(E entry) {
        beforeInsert(entry);
        var key = getUniqueEntryKey(entry);
        bTree.insert(key);
    }

    public void delete(E entry) {
        var key = getUniqueEntryKey(entry);
        bTree.delete(key);

    }

    public void update(E oldEntry, E newEntry) {
        if (oldEntry.getPrimaryKey() != newEntry.getPrimaryKey()) {
            throw new IllegalArgumentException();
        }
        delete(oldEntry);
        insert(newEntry);
    }

    public long persist(BlockWriter blockWriter) {
        bTree.persistChanges(blockWriter);
        long indexrootId = bTree.getRootId().orElseThrow();
        return indexrootId;
    }

    public abstract boolean isUnique();


    public abstract I getUniqueEntryKey(E entry);

    public abstract KeyRange<I> getSearchKey(E entry);

}
