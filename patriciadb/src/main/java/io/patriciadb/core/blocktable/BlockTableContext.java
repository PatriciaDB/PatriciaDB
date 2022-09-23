package io.patriciadb.core.blocktable;

import io.patriciadb.index.btree.BTree;
import io.patriciadb.table.TableContext;
import io.patriciadb.core.blocktable.index.BlockHashIndex;
import io.patriciadb.core.blocktable.index.BlockIdIndex;
import io.patriciadb.core.blocktable.index.ParentBlockHashIndex;
import io.patriciadb.table.Index;
import io.patriciadb.utils.Serializer;

import java.util.List;

public class BlockTableContext implements TableContext<BlockEntity> {


    private final long tableMetadataId;
    private final BlockHashIndex blockHashIndex;
    private final ParentBlockHashIndex parentBlockHashIndex;
    private final BlockIdIndex blockIdIndex;
    private final BTree<Long> primaryKey;
    private final List<Index<BlockEntity>> indexList;

    public BlockTableContext(long tableMetadataId, BTree<Long> primaryKey, BlockHashIndex blockHashIndex, ParentBlockHashIndex parentBlockHashIndex, BlockIdIndex blockIdIndex) {
        this.blockHashIndex = blockHashIndex;
        this.parentBlockHashIndex = parentBlockHashIndex;
        this.primaryKey = primaryKey;
        indexList = List.of(blockHashIndex, parentBlockHashIndex, blockIdIndex);
        this.tableMetadataId = tableMetadataId;
        this.blockIdIndex = blockIdIndex;
    }

    public BlockHashIndex getSnapshotIdIndex() {
            return blockHashIndex;
    }

    public ParentBlockHashIndex getParentBlockHashIndex() {
        return parentBlockHashIndex;
    }

    public BlockIdIndex getBlockIdIndex() {
        return blockIdIndex;
    }

    @Override
    public Serializer<BlockEntity> entitySerializer() {
        return BlockEntity.SERIALIZER;
    }

    @Override
    public List<Index<BlockEntity>> getIndexList() {
        return indexList;
    }

    @Override
    public BTree<Long> getPrimaryIndex() {
        return primaryKey;
    }

    @Override
    public long getTableId() {
        return tableMetadataId;
    }
}
