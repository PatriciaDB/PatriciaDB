package io.patriciadb.core.blocktable;

import io.patriciadb.fs.BlockReader;
import io.patriciadb.table.TableOperations;

import java.util.List;
import java.util.Optional;

public class BlockTableOperations {

    public static Optional<BlockEntity> findBySnapshotId(BlockTableContext context, BlockReader reader, byte[] snapshotId) {
        return context.getSnapshotIdIndex()
                .getBySnapshotId(snapshotId)
                .flatMap(id -> TableOperations.get(context, reader, id));
    }

    public static List<BlockEntity> findByParentHash(BlockTableContext context, BlockReader reader, byte[] parentHash) {
        return context.getParentBlockHashIndex()
                .getByParentHash(parentHash)
                .stream()
                .map(id -> TableOperations.get(context, reader, id))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    public static List<BlockEntity> findByBlockId(BlockTableContext context, BlockReader reader, long blockId) {
        return context.getBlockIdIndex()
                .getByBlockId(blockId)
                .stream()
                .map(id -> TableOperations.get(context, reader, id))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }
}
