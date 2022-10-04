package io.patriciadb.core.transactionstable;

import io.patriciadb.fs.BlockReader;
import io.patriciadb.table.TableOperations;

import java.util.List;
import java.util.Optional;

public class TransactionReadTableImp implements TransactionTableRead {
    private final TransactionTableContext context;
    private final BlockReader reader;

    public TransactionReadTableImp(TransactionTableContext context, BlockReader reader) {
        this.context = context;
        this.reader = reader;
    }

    @Override
    public Optional<TransactionEntity> findByBlockHash(byte[] snapshotId) {
        return TransactionTableOperations.findBySnapshotId(context, reader, snapshotId);
    }

    @Override
    public List<TransactionEntity> findByParentBlockHash(byte[] parentBlockhash) {
        return TransactionTableOperations.findByParentHash(context, reader, parentBlockhash);
    }

    @Override
    public List<TransactionEntity> findByBlockNumber(long blockNumber) {
        return TransactionTableOperations.findByBlockId(context, reader,blockNumber);
    }


    @Override
    public Optional<TransactionEntity> get(long primaryKey) {
        return TableOperations.get(context, reader, primaryKey);
    }
}
