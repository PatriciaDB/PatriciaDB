package io.patriciadb.core.blocktable;

import io.patriciadb.core.blocktable.index.BlockHashIndex;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.fs.BlockWriter;
import io.patriciadb.index.btree.BTree;
import io.patriciadb.table.TableFactory;
import io.patriciadb.table.TableMetadata;
import io.patriciadb.utils.Serializers;
import io.patriciadb.core.blocktable.index.BlockIdIndex;
import io.patriciadb.core.blocktable.index.ParentBlockHashIndex;

public class BlockTableFactory implements TableFactory<BlockEntity, BlockTableRead, BlockTable, BlockTableContext> {

    public static final BlockTableFactory INSTANCE = new BlockTableFactory();

    private BlockTableFactory() {

    }

    @Override
    public BlockTableContext createTableContext(BlockReader reader, TableMetadata metadata, long tableMetadataId) {
        long blockHashIdxRootNode = metadata.getSecondaryIndexRootNodeId(BlockHashIndex.ID);
        long parentBlockHashIndexRootNode = metadata.getSecondaryIndexRootNodeId(ParentBlockHashIndex.ID);
        long blockIdIndexRootNode = metadata.getSecondaryIndexRootNodeId(BlockIdIndex.ID);

        BlockHashIndex blockHashIndex = BlockHashIndex.openOrCreate(reader, blockHashIdxRootNode);
        ParentBlockHashIndex parentBlockHashIndex = ParentBlockHashIndex.openOrCreate(reader, parentBlockHashIndexRootNode);
        BlockIdIndex blockIdIndex = BlockIdIndex.openOrCreate(reader, blockIdIndexRootNode);

        long primaryIndexRootId =metadata.getPrimaryKey();
        BTree<Long> primaryIndex = BTree.openOrCreate(primaryIndexRootId, Serializers.LONG_SERIALIZER, reader);
        return new BlockTableContext(tableMetadataId, primaryIndex, blockHashIndex, parentBlockHashIndex, blockIdIndex);
    }

    @Override
    public BlockTableRead openReadOnly(BlockReader reader, BlockTableContext context) {
        return new BlockTableReadImp(context, reader);
    }

    @Override
    public BlockTable open(BlockWriter writer, BlockTableContext context) {
        return new BlockTableImp(context, writer);
    }
}
