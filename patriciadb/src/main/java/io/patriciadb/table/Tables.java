package io.patriciadb.table;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.fs.BlockReader;

import java.util.HashMap;

public class Tables {


    public static <E extends Entity, T extends Table<E>, TR extends TableRead<E>, C extends TableContext<E>>
    TR openReadOnly(TableFactory<E, TR, T, C> tableFactory, BlockReader blockReader, long tableMetadataId) {
        var metadata = loadMetadata(blockReader, tableMetadataId);
        var context = tableFactory.createTableContext(blockReader, metadata, tableMetadataId);
        return tableFactory.openReadOnly(blockReader, context);
    }

    public static <E extends Entity, T extends Table<E>, TR extends TableRead<E>, C extends TableContext<E>>
    T open(TableFactory<E, TR, T, C> tableFactory, BlockWriter blockWriter, long tableMetadataId) {
        var metadata = loadMetadata(blockWriter, tableMetadataId);
        var context = tableFactory.createTableContext(blockWriter, metadata,tableMetadataId);
        return tableFactory.open(blockWriter, context);
    }

    public static <E extends Entity, T extends Table<E>, TR extends TableRead<E>, C extends TableContext<E>> T
    createNew(TableFactory<E, TR, T, C> tableFactory, BlockWriter blockWriter, long tableMetadataId) {
        var buffer = blockWriter.read(tableMetadataId);
        if (buffer != null) {
            throw new IllegalStateException("Table with ID " + tableMetadataId + " already exists");
        }
        var tableMetadata = new TableMetadata(new HashMap<>());
        blockWriter.overwrite(tableMetadataId, TableMetadata.SERIALIZER.serialize(tableMetadata));
        return open(tableFactory, blockWriter, tableMetadataId);
    }

    private static TableMetadata loadMetadata(BlockReader reader, long blockId) {
        var buffer = reader.read(blockId);
        if (buffer == null) {
            throw new IllegalStateException("Table " + blockId + " not found");
        }
        return TableMetadata.SERIALIZER.deserialize(buffer);
    }
}
