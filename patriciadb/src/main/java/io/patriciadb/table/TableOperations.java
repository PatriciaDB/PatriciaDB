package io.patriciadb.table;

import io.patriciadb.fs.BlockWriter;
import io.patriciadb.fs.BlockReader;

import java.util.Optional;

public class TableOperations {

    public static <E extends Entity> void insert(TableContext<E> context, BlockWriter writer, E entry) {
        for (var index : context.getIndexList()) {
            index.beforeInsert(entry);
        }
        var entryBuffer = context.entitySerializer().serialize(entry);
        long primaryKey = writer.write(entryBuffer);
        context.getPrimaryIndex().insert(primaryKey);
        entry.setPrimaryKey(primaryKey);
        for (var index : context.getIndexList()) {
            index.insert(entry);
        }
    }

    public static <E extends Entity> boolean delete(TableContext<E> context, BlockWriter writer, long primaryKey) {
        var accountOpt = get(context, writer, primaryKey);
        if (!accountOpt.isPresent()) {
            return false;
        }
        context.getPrimaryIndex().delete(primaryKey);
        for (var index : context.getIndexList()) {
            index.delete(accountOpt.get());
        }
        writer.delete(primaryKey);
        return true;
    }

    public static <E extends Entity> void update(TableContext<E> context, BlockWriter writer, E entry) {
        if (entry.getPrimaryKey() <= 0) {
            throw new IllegalArgumentException("invalid primary key " + entry.getPrimaryKey());
        }
        var precVersion = get(context, writer, entry.getPrimaryKey());
        if (precVersion.isEmpty()) {
            throw new IllegalStateException("Precedent version of the entry not found");
        }
        for (var index : context.getIndexList()) {
            index.update(precVersion.get(), entry);
        }
        writer.overwrite(entry.getPrimaryKey(), context.entitySerializer().serialize(entry));
    }

    public static <E extends Entity> Optional<E> get(TableContext<E> context, BlockReader readers, long primaryKey) {
        var buffer = readers.read(primaryKey);
        if (buffer == null) {
            return Optional.empty();
        }
        var account = context.entitySerializer().deserialize(buffer);
        account.setPrimaryKey(primaryKey);
        return Optional.of(account);
    }

    public static <E extends Entity> void persist(TableContext<E> context, BlockWriter writer) {
        TableMetadata tableMetadata = new TableMetadata();
        context.getPrimaryIndex().persistChanges(writer);
        long primaryIndexRootId = context.getPrimaryIndex().getRootId().orElseThrow();
        tableMetadata.setPrimaryKey(primaryIndexRootId);

        for (var index : context.getIndexList()) {
            long indexRootId = index.persist(writer);
            tableMetadata.setSecondaryIndexNodeId(index.getIndexName(), indexRootId);
        }
        writer.overwrite(context.getTableId(), TableMetadata.SERIALIZER.serialize(tableMetadata));
    }


}
