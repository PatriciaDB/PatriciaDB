package io.patriciadb.core.blocktable.index;

import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.index.btree.BTree;
import io.patriciadb.index.btree.KeyRange;
import io.patriciadb.table.BTreeIndexAbs;

import java.util.List;

public class ParentBlockHashIndex extends BTreeIndexAbs<BlockEntity, ParentBlockHashIndexKey> {
    public static final String ID = "parentBlockHashIdx";


    private ParentBlockHashIndex(BTree<ParentBlockHashIndexKey> bTree) {
        super(bTree);
    }

    public static ParentBlockHashIndex openOrCreate(BlockReader reader, long rootNodeId) {
        BTree<ParentBlockHashIndexKey> btree = BTree.openOrCreate(rootNodeId, ParentBlockHashIndexKey.SERIALIZER, reader);
        return new ParentBlockHashIndex(btree);
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public ParentBlockHashIndexKey getUniqueEntryKey(BlockEntity entry) {
        return new ParentBlockHashIndexKey(entry.getParentBlockHash(), entry.getPrimaryKey());
    }

    @Override
    public KeyRange<ParentBlockHashIndexKey> getSearchKey(BlockEntity entry) {
        return KeyRange.of(
                new ParentBlockHashIndexKey(entry.getParentBlockHash(), Long.MIN_VALUE),
                new ParentBlockHashIndexKey(entry.getParentBlockHash(), Long.MAX_VALUE)
        );
    }

    private KeyRange<ParentBlockHashIndexKey> getSearchKey(byte[] parentBlockHash) {
        return KeyRange.of(
                new ParentBlockHashIndexKey(parentBlockHash, Long.MIN_VALUE),
                new ParentBlockHashIndexKey(parentBlockHash, Long.MAX_VALUE)
        );
    }

    @Override
    public String getIndexName() {
        return ID;
    }

    public List<Long> getByParentHash(byte[] parentBlockHash) {
        var result = bTree.find(getSearchKey(parentBlockHash));
        return result.stream().map(ParentBlockHashIndexKey::primaryKey).toList();
    }

}
