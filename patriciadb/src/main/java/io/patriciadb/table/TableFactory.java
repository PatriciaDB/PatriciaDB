package io.patriciadb.table;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.fs.BlockReader;

public interface TableFactory<E extends Entity, TR extends TableRead<E>, T extends Table<E>, C extends TableContext<E>> {


    C createTableContext(BlockReader reader,TableMetadata metadata, long tableMetadataId);


    TR openReadOnly(BlockReader reader, C context);

    T open(BlockWriter writer, C context);
}
