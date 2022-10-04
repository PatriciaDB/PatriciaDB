package io.patriciadb.core.transactionstable;

import io.patriciadb.fs.BlockReader;
import io.patriciadb.table.TableOperations;

import java.util.List;
import java.util.Optional;

public class TransactionTableOperations {

    public static Optional<TransactionEntity> findBySnapshotId(TransactionTableContext context, BlockReader reader, byte[] transactionId) {
        return context.getSnapshotIdIndex()
                .getBySnapshotId(transactionId)
                .flatMap(id -> TableOperations.get(context, reader, id));
    }

    public static List<TransactionEntity> findByParentHash(TransactionTableContext context, BlockReader reader, byte[] parentId) {
        return context.getParentBlockHashIndex()
                .getByParentHash(parentId)
                .stream()
                .map(id -> TableOperations.get(context, reader, id))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    public static List<TransactionEntity> findByBlockId(TransactionTableContext context, BlockReader reader, long blockId) {
        return context.getBlockIdIndex()
                .getByBlockId(blockId)
                .stream()
                .map(id -> TableOperations.get(context, reader, id))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }
}
