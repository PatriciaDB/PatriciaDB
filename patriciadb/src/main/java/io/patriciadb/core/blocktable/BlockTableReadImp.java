package io.patriciadb.core.blocktable;

import io.patriciadb.fs.BlockReader;
import io.patriciadb.table.TableOperations;

import java.util.List;
import java.util.Optional;

public class BlockTableReadImp implements BlockTableRead {
    private final BlockTableContext context;
    private final BlockReader reader;

    public BlockTableReadImp(BlockTableContext context, BlockReader reader) {
        this.context = context;
        this.reader = reader;
    }

    @Override
    public Optional<BlockEntity> findByBlockHash(byte[] snapshotId) {
        return BlockTableOperations.findBySnapshotId(context, reader, snapshotId);
    }

    @Override
    public List<BlockEntity> findByParentBlockHash(byte[] parentBlockhash) {
        return BlockTableOperations.findByParentHash(context, reader, parentBlockhash);
    }

    @Override
    public List<BlockEntity> findByBlockNumber(long blockNumber) {
        return BlockTableOperations.findByBlockId(context, reader,blockNumber);
    }


    @Override
    public Optional<BlockEntity> get(long primaryKey) {
        return TableOperations.get(context, reader, primaryKey);
    }
}
