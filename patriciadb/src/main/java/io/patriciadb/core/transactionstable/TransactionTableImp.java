package io.patriciadb.core.transactionstable;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.table.TableOperations;

import java.util.List;
import java.util.Optional;

public class TransactionTableImp implements TransactionTable {
    private final TransactionTableContext context;
    private final BlockWriter writer;

    public TransactionTableImp(TransactionTableContext context, BlockWriter writer) {
        this.context = context;
        this.writer = writer;
    }

    @Override
    public Optional<TransactionEntity> findByBlockHash(byte[] blockHash) {
        return TransactionTableOperations.findBySnapshotId(context, writer, blockHash);
    }

    @Override
    public List<TransactionEntity> findByParentBlockHash(byte[] parentBlockhash) {
        return TransactionTableOperations.findByParentHash(context, writer, parentBlockhash);
    }

    @Override
    public List<TransactionEntity> findByBlockNumber(long blockNumber) {
        return TransactionTableOperations.findByBlockId(context, writer,blockNumber);
    }

    @Override
    public void insert(TransactionEntity entity) {
        TableOperations.insert(context, writer, entity);
    }

    @Override
    public void update(TransactionEntity entity) {
        TableOperations.update(context, writer, entity);
    }

    @Override
    public void delete(long primaryKey) {
        TableOperations.delete(context, writer, primaryKey);
    }

    @Override
    public Optional<TransactionEntity> get(long primaryKey) {
        return TableOperations.get(context, writer, primaryKey);
    }

    @Override
    public void persist() {
        TableOperations.persist(context, writer);
    }
}
