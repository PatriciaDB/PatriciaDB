package io.patriciadb.core.transactionstable.index;

import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.index.btree.BTree;
import io.patriciadb.index.btree.KeyRange;
import io.patriciadb.table.BTreeIndexAbs;

import java.util.List;

public class ParentTransactionIdIndex extends BTreeIndexAbs<TransactionEntity, ParentTransactionIdIndexKey> {
    public static final String ID = "parentBlockHashIdx";


    private ParentTransactionIdIndex(BTree<ParentTransactionIdIndexKey> bTree) {
        super(bTree);
    }

    public static ParentTransactionIdIndex openOrCreate(BlockReader reader, long rootNodeId) {
        BTree<ParentTransactionIdIndexKey> btree = BTree.openOrCreate(rootNodeId, ParentTransactionIdIndexKey.SERIALIZER, reader);
        return new ParentTransactionIdIndex(btree);
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public ParentTransactionIdIndexKey getUniqueEntryKey(TransactionEntity entry) {
        return new ParentTransactionIdIndexKey(entry.getParentTransactionId(), entry.getPrimaryKey());
    }

    @Override
    public KeyRange<ParentTransactionIdIndexKey> getSearchKey(TransactionEntity entry) {
        return KeyRange.of(
                new ParentTransactionIdIndexKey(entry.getParentTransactionId(), Long.MIN_VALUE),
                new ParentTransactionIdIndexKey(entry.getParentTransactionId(), Long.MAX_VALUE)
        );
    }

    private KeyRange<ParentTransactionIdIndexKey> getSearchKey(byte[] parentBlockHash) {
        return KeyRange.of(
                new ParentTransactionIdIndexKey(parentBlockHash, Long.MIN_VALUE),
                new ParentTransactionIdIndexKey(parentBlockHash, Long.MAX_VALUE)
        );
    }

    @Override
    public String getIndexName() {
        return ID;
    }

    public List<Long> getByParentHash(byte[] parentBlockHash) {
        var result = bTree.find(getSearchKey(parentBlockHash));
        return result.stream().map(ParentTransactionIdIndexKey::primaryKey).toList();
    }

}
