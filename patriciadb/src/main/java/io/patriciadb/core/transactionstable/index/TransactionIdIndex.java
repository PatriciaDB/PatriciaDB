package io.patriciadb.core.transactionstable.index;

import io.patriciadb.index.btree.BTree;
import io.patriciadb.index.btree.KeyRange;
import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.table.BTreeIndexAbs;

import java.util.Optional;

public class TransactionIdIndex extends BTreeIndexAbs<TransactionEntity, TransactionIdIndexKey> {

    public static final String ID = "transactionIdIdx";

    private TransactionIdIndex(BTree<TransactionIdIndexKey> bTree) {
        super(bTree);
    }

    public static TransactionIdIndex openOrCreate(BlockReader reader, long rootNodeId) {
        BTree<TransactionIdIndexKey> btree =  BTree.openOrCreate(rootNodeId, TransactionIdIndexKey.SERIALIZER, reader);
        return new TransactionIdIndex(btree);
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public TransactionIdIndexKey getUniqueEntryKey(TransactionEntity entry) {
        return new TransactionIdIndexKey(entry.getTransactionId(), entry.getPrimaryKey());
    }

    @Override
    public KeyRange<TransactionIdIndexKey> getSearchKey(TransactionEntity entry) {
        return KeyRange.of(
                new TransactionIdIndexKey(entry.getTransactionId(), Long.MIN_VALUE),
                new TransactionIdIndexKey(entry.getTransactionId(), Long.MAX_VALUE)
                );
    }

    private KeyRange<TransactionIdIndexKey> getSearchKey(byte[] snapshotId ) {
        return KeyRange.of(
                new TransactionIdIndexKey(snapshotId, Long.MIN_VALUE),
                new TransactionIdIndexKey(snapshotId, Long.MAX_VALUE)
        );
    }

    @Override
    public String getIndexName() {
        return TransactionIdIndex.ID;
    }

    public Optional<Long> getBySnapshotId(byte[] snapshotId) {
        var result = bTree.find(getSearchKey(snapshotId));
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0).primaryKey());
    }
}
