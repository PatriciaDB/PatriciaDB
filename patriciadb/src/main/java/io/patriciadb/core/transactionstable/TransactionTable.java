package io.patriciadb.core.transactionstable;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.table.Table;
import io.patriciadb.table.Tables;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.fs.PatriciaFileSystem;

public interface TransactionTable extends Table<TransactionEntity>, TransactionTableRead {
    public static final long MAIN_TABLE_ID = 1;

    static TransactionTable open(BlockWriter writer) {
        return Tables.open(TransactionTableFactory.INSTANCE, writer, MAIN_TABLE_ID);
    }

    static TransactionTableRead openReadOnly(BlockReader reader) {
        return Tables.openReadOnly(TransactionTableFactory.INSTANCE, reader, MAIN_TABLE_ID);
    }

    static void initialiseTable(PatriciaFileSystem fs) {
        var transaction = fs.startTransaction();
        try {
            Tables.createNew(TransactionTableFactory.INSTANCE, transaction, MAIN_TABLE_ID);
            transaction.commit();
        } finally {
            transaction.release();
        }
    }
}
