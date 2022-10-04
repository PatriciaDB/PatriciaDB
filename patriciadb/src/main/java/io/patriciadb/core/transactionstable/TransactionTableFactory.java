package io.patriciadb.core.transactionstable;

import io.patriciadb.core.transactionstable.index.TransactionIdIndex;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.fs.BlockWriter;
import io.patriciadb.index.btree.BTree;
import io.patriciadb.table.TableFactory;
import io.patriciadb.table.TableMetadata;
import io.patriciadb.utils.Serializers;
import io.patriciadb.core.transactionstable.index.TransactionBlockNumberIndex;
import io.patriciadb.core.transactionstable.index.ParentTransactionIdIndex;

public class TransactionTableFactory implements TableFactory<TransactionEntity, TransactionTableRead, TransactionTable, TransactionTableContext> {

    public static final TransactionTableFactory INSTANCE = new TransactionTableFactory();

    private TransactionTableFactory() {

    }

    @Override
    public TransactionTableContext createTableContext(BlockReader reader, TableMetadata metadata, long tableMetadataId) {
        long blockHashIdxRootNode = metadata.getSecondaryIndexRootNodeId(TransactionIdIndex.ID);
        long parentBlockHashIndexRootNode = metadata.getSecondaryIndexRootNodeId(ParentTransactionIdIndex.ID);
        long blockIdIndexRootNode = metadata.getSecondaryIndexRootNodeId(TransactionBlockNumberIndex.ID);

        TransactionIdIndex transactionIdIndex = TransactionIdIndex.openOrCreate(reader, blockHashIdxRootNode);
        ParentTransactionIdIndex parentTransactionIdIndex = ParentTransactionIdIndex.openOrCreate(reader, parentBlockHashIndexRootNode);
        TransactionBlockNumberIndex transactionBlockNumberIndex = TransactionBlockNumberIndex.openOrCreate(reader, blockIdIndexRootNode);

        long primaryIndexRootId =metadata.getPrimaryKey();
        BTree<Long> primaryIndex = BTree.openOrCreate(primaryIndexRootId, Serializers.LONG_SERIALIZER, reader);
        return new TransactionTableContext(tableMetadataId, primaryIndex, transactionIdIndex, parentTransactionIdIndex, transactionBlockNumberIndex);
    }

    @Override
    public TransactionTableRead openReadOnly(BlockReader reader, TransactionTableContext context) {
        return new TransactionReadTableImp(context, reader);
    }

    @Override
    public TransactionTable open(BlockWriter writer, TransactionTableContext context) {
        return new TransactionTableImp(context, writer);
    }
}
