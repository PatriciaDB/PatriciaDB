package io.patriciadb.core.blocktable;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.table.TableOperations;

import java.util.List;
import java.util.Optional;

public class BlockTableImp implements BlockTable {
    private final BlockTableContext context;
    private final BlockWriter writer;

    public BlockTableImp(BlockTableContext context, BlockWriter writer) {
        this.context = context;
        this.writer = writer;
    }

    @Override
    public Optional<BlockEntity> findByBlockHash(byte[] blockHash) {
        return BlockTableOperations.findBySnapshotId(context, writer, blockHash);
    }

    @Override
    public List<BlockEntity> findByParentBlockHash(byte[] parentBlockhash) {
        return BlockTableOperations.findByParentHash(context, writer, parentBlockhash);
    }

    @Override
    public List<BlockEntity> findByBlockNumber(long blockNumber) {
        return BlockTableOperations.findByBlockId(context, writer,blockNumber);
    }

    @Override
    public void insert(BlockEntity entity) {
        TableOperations.insert(context, writer, entity);
    }

    @Override
    public void update(BlockEntity entity) {
        TableOperations.update(context, writer, entity);
    }

    @Override
    public void delete(long primaryKey) {
        TableOperations.delete(context, writer, primaryKey);
    }

    @Override
    public Optional<BlockEntity> get(long primaryKey) {
        return TableOperations.get(context, writer, primaryKey);
    }

    @Override
    public void persist() {
        TableOperations.persist(context, writer);
    }
}
