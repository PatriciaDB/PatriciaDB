package io.patriciadb.core.transactionstable;

import io.patriciadb.index.btree.BTree;
import io.patriciadb.table.TableContext;
import io.patriciadb.core.transactionstable.index.TransactionIdIndex;
import io.patriciadb.core.transactionstable.index.TransactionBlockNumberIndex;
import io.patriciadb.core.transactionstable.index.ParentTransactionIdIndex;
import io.patriciadb.table.Index;
import io.patriciadb.utils.Serializer;

import java.util.List;

public class TransactionTableContext implements TableContext<TransactionEntity> {


    private final long tableMetadataId;
    private final TransactionIdIndex transactionIdIndex;
    private final ParentTransactionIdIndex parentTransactionIdIndex;
    private final TransactionBlockNumberIndex transactionBlockNumberIndex;
    private final BTree<Long> primaryKey;
    private final List<Index<TransactionEntity>> indexList;

    public TransactionTableContext(long tableMetadataId, BTree<Long> primaryKey, TransactionIdIndex transactionIdIndex, ParentTransactionIdIndex parentTransactionIdIndex, TransactionBlockNumberIndex transactionBlockNumberIndex) {
        this.transactionIdIndex = transactionIdIndex;
        this.parentTransactionIdIndex = parentTransactionIdIndex;
        this.primaryKey = primaryKey;
        indexList = List.of(transactionIdIndex, parentTransactionIdIndex, transactionBlockNumberIndex);
        this.tableMetadataId = tableMetadataId;
        this.transactionBlockNumberIndex = transactionBlockNumberIndex;
    }

    public TransactionIdIndex getSnapshotIdIndex() {
            return transactionIdIndex;
    }

    public ParentTransactionIdIndex getParentBlockHashIndex() {
        return parentTransactionIdIndex;
    }

    public TransactionBlockNumberIndex getBlockIdIndex() {
        return transactionBlockNumberIndex;
    }

    @Override
    public Serializer<TransactionEntity> entitySerializer() {
        return TransactionEntity.SERIALIZER;
    }

    @Override
    public List<Index<TransactionEntity>> getIndexList() {
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
