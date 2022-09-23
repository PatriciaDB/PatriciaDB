package io.patriciadb.core.blocktable.index;

import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.index.btree.BTree;
import io.patriciadb.index.btree.KeyRange;
import io.patriciadb.table.BTreeIndexAbs;

import java.util.List;

public class BlockIdIndex extends BTreeIndexAbs<BlockEntity, BlockIdIndexKey> {
    public static final String ID = "blockIdIdx";


    private BlockIdIndex(BTree<BlockIdIndexKey> bTree) {
        super(bTree);
    }

    public static BlockIdIndex openOrCreate(BlockReader reader, long rootNodeId) {
        BTree<BlockIdIndexKey> btree = BTree.openOrCreate(rootNodeId, BlockIdIndexKey.SERIALIZER, reader);
        return new BlockIdIndex(btree);
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public BlockIdIndexKey getUniqueEntryKey(BlockEntity entry) {
        return new BlockIdIndexKey(entry.getBlockNumber(), entry.getPrimaryKey());
    }

    @Override
    public KeyRange<BlockIdIndexKey> getSearchKey(BlockEntity entry) {
        return KeyRange.of(
                new BlockIdIndexKey(entry.getBlockNumber(), Long.MIN_VALUE),
                new BlockIdIndexKey(entry.getBlockNumber(), Long.MAX_VALUE)
        );
    }

    private KeyRange<BlockIdIndexKey> getSearchKey(long blockId) {
        return KeyRange.of(
                new BlockIdIndexKey(blockId, Long.MIN_VALUE),
                new BlockIdIndexKey(blockId, Long.MAX_VALUE)
        );
    }

    @Override
    public String getIndexName() {
        return ID;
    }

    public List<Long> getByBlockId(long blockId) {
        var result = bTree.find(getSearchKey(blockId));
        return result.stream().map(BlockIdIndexKey::primaryKey).toList();
    }

}
