package io.patriciadb.core.blocktable;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.table.Table;
import io.patriciadb.table.Tables;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.fs.PatriciaFileSystem;

public interface BlockTable extends Table<BlockEntity>, BlockTableRead {
    public static final long MAIN_TABLE_ID = 1;

    static BlockTable open(BlockWriter writer) {
        return Tables.open(BlockTableFactory.INSTANCE, writer, MAIN_TABLE_ID);
    }

    static BlockTableRead openReadOnly(BlockReader reader) {
        return Tables.openReadOnly(BlockTableFactory.INSTANCE, reader, MAIN_TABLE_ID);
    }

    static void initialiseTable(PatriciaFileSystem fs) {
        var transaction = fs.startTransaction();
        try {
            Tables.createNew(BlockTableFactory.INSTANCE, transaction, MAIN_TABLE_ID);
            transaction.commit();
        } finally {
            transaction.release();
        }
    }
}
