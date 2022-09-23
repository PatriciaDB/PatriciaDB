package io.patriciadb.core.blocktable.index;

import io.patriciadb.index.btree.BTree;
import io.patriciadb.index.btree.KeyRange;
import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.table.BTreeIndexAbs;

import java.util.Optional;

public class BlockHashIndex extends BTreeIndexAbs<BlockEntity, BlockHashIndexKey> {

    public static final String ID = "blockHashIdx";

    private BlockHashIndex(BTree<BlockHashIndexKey> bTree) {
        super(bTree);
    }

    public static BlockHashIndex openOrCreate(BlockReader reader, long rootNodeId) {
        BTree<BlockHashIndexKey> btree =  BTree.openOrCreate(rootNodeId, BlockHashIndexKey.SERIALIZER, reader);
        return new BlockHashIndex(btree);
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public BlockHashIndexKey getUniqueEntryKey(BlockEntity entry) {
        return new BlockHashIndexKey(entry.getBlockHash(), entry.getPrimaryKey());
    }

    @Override
    public KeyRange<BlockHashIndexKey> getSearchKey(BlockEntity entry) {
        return KeyRange.of(
                new BlockHashIndexKey(entry.getBlockHash(), Long.MIN_VALUE),
                new BlockHashIndexKey(entry.getBlockHash(), Long.MAX_VALUE)
                );
    }

    private KeyRange<BlockHashIndexKey> getSearchKey(byte[] snapshotId ) {
        return KeyRange.of(
                new BlockHashIndexKey(snapshotId, Long.MIN_VALUE),
                new BlockHashIndexKey(snapshotId, Long.MAX_VALUE)
        );
    }

    @Override
    public String getIndexName() {
        return BlockHashIndex.ID;
    }

    public Optional<Long> getBySnapshotId(byte[] snapshotId) {
        var result = bTree.find(getSearchKey(snapshotId));
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0).primaryKey());
    }
}
