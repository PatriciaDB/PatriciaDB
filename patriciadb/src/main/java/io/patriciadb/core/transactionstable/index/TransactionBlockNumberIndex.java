package io.patriciadb.core.transactionstable.index;

import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.index.btree.BTree;
import io.patriciadb.index.btree.KeyRange;
import io.patriciadb.table.BTreeIndexAbs;

import java.util.List;

public class TransactionBlockNumberIndex extends BTreeIndexAbs<TransactionEntity, TransactionBlockNumberIndexKey> {
    public static final String ID = "transactionBlockNumberIdx";


    private TransactionBlockNumberIndex(BTree<TransactionBlockNumberIndexKey> bTree) {
        super(bTree);
    }

    public static TransactionBlockNumberIndex openOrCreate(BlockReader reader, long rootNodeId) {
        BTree<TransactionBlockNumberIndexKey> btree = BTree.openOrCreate(rootNodeId, TransactionBlockNumberIndexKey.SERIALIZER, reader);
        return new TransactionBlockNumberIndex(btree);
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public TransactionBlockNumberIndexKey getUniqueEntryKey(TransactionEntity entry) {
        return new TransactionBlockNumberIndexKey(entry.getBlockNumber(), entry.getPrimaryKey());
    }

    @Override
    public KeyRange<TransactionBlockNumberIndexKey> getSearchKey(TransactionEntity entry) {
        return KeyRange.of(
                new TransactionBlockNumberIndexKey(entry.getBlockNumber(), Long.MIN_VALUE),
                new TransactionBlockNumberIndexKey(entry.getBlockNumber(), Long.MAX_VALUE)
        );
    }

    private KeyRange<TransactionBlockNumberIndexKey> getSearchKey(long blockId) {
        return KeyRange.of(
                new TransactionBlockNumberIndexKey(blockId, Long.MIN_VALUE),
                new TransactionBlockNumberIndexKey(blockId, Long.MAX_VALUE)
        );
    }

    @Override
    public String getIndexName() {
        return ID;
    }

    public List<Long> getByBlockId(long blockId) {
        var result = bTree.find(getSearchKey(blockId));
        return result.stream().map(TransactionBlockNumberIndexKey::primaryKey).toList();
    }

}
