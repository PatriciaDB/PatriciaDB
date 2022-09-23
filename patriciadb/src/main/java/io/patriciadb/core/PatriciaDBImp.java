package io.patriciadb.core;

import io.patriciadb.BlockInfo;
import io.patriciadb.PatriciaDB;
import io.patriciadb.Snapshot;
import io.patriciadb.Transaction;
import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.core.blocktable.BlockTable;
import io.patriciadb.fs.PatriciaFileSystem;
import io.patriciadb.core.utils.BlockEntityConverter;
import io.patriciadb.utils.BitMapUtils;
import io.patriciadb.utils.ExceptionUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class PatriciaDBImp implements PatriciaDB {

    private final static Logger log = LoggerFactory.getLogger(PatriciaDBImp.class);

    private final PatriciaFileSystem fileSystem;

    public PatriciaDBImp(PatriciaFileSystem fileSystem) {
        this.fileSystem = fileSystem;
        validate();
    }

    private void validate() {
        var tr = fileSystem.getSnapshot();
        try {
            var table = BlockTable.openReadOnly(tr);
        } finally {
            tr.release();
        }
    }

    @Override
    public Snapshot readTransaction(byte[] blockHash) {
        return null;
    }

    @Override
    public Transaction startTransaction(byte[] parentBlockHash) {
        var tr = fileSystem.startTransaction();
        try {
            var blockTable = BlockTable.open(tr);
            var parentBlockInfo = blockTable.findByBlockHash(parentBlockHash);
            if (parentBlockInfo.isEmpty()) {
                throw new IllegalArgumentException("BlockHash not found " + Arrays.toString(parentBlockHash));
            }
            return new TransactionImp(tr, blockTable, parentBlockInfo.get());
        } catch (Throwable t) {
            tr.release();
            throw ExceptionUtils.sneakyThrow(t);
        }
    }

    @Override
    public Transaction startTransaction() {
        var tr = fileSystem.startTransaction();
        try {
            var blockTable = BlockTable.open(tr);
            var parentBlock = new BlockEntity();
            parentBlock.setExtra("");
            parentBlock.setCreationTime(Instant.now());
            parentBlock.setBlockHash(new byte[0]);
            parentBlock.setParentBlockHash(new byte[0]);
            parentBlock.setIndexRootNodeId(0);
            return new TransactionImp(tr, blockTable, parentBlock);
        } catch (Throwable t) {
            tr.release();
            throw ExceptionUtils.sneakyThrow(t);
        }
    }

    @Override
    public void purgeBlockData(byte[] blockHash) {
        fileSystem.startTransaction(tr -> {
            var blockTable = BlockTable.open(tr);
            var blockOpt = blockTable.findByBlockHash(blockHash);
            if (blockOpt.isEmpty()) {
                throw new IllegalArgumentException("Block not found");
            }
            var childBlocks = blockTable.findByParentBlockHash(blockHash);
            var block = blockOpt.get();
            var newNodeIds = BitMapUtils.deserialize(block.getNewNodeIds());
            var lostNodeIds = BitMapUtils.deserialize(block.getLostNodeIds());

            var nodeToDelete = new Roaring64NavigableMap(false);
            nodeToDelete.or(newNodeIds);
            for (var childBlock : childBlocks) {
                childBlock.setParentBlockHash(block.getParentBlockHash());
                var childLostNodes = BitMapUtils.deserialize(childBlock.getLostNodeIds());
                var childNewNodes = BitMapUtils.deserialize(childBlock.getNewNodeIds());
                nodeToDelete.and(childLostNodes);
                childNewNodes.or(newNodeIds);
                childNewNodes.andNot(childLostNodes);
                childLostNodes.andNot(newNodeIds);
                childLostNodes.or(lostNodeIds);

                childBlock.setNewNodeIds(BitMapUtils.serialize(childNewNodes));
                childBlock.setLostNodeIds(BitMapUtils.serialize(childLostNodes));
            }
            log.debug("Deleting {} nodes from transaction history id {}", nodeToDelete.getLongCardinality(), Arrays.toString(blockHash));
            nodeToDelete.forEach(tr::delete);

            for (var childBlock : childBlocks) {
                blockTable.update(childBlock);
            }
            blockTable.delete(block.getPrimaryKey());
            return true;
        });
    }

    @Override
    public Optional<BlockInfo> getMetadata(byte[] blockHash) {
        return fileSystem.getSnapshot(fsSnapshot -> {
            return BlockTable.openReadOnly(fsSnapshot).findByBlockHash(blockHash).map(BlockEntityConverter::fromBlockEntity);
        });
    }

    @Override
    public List<? extends BlockInfo> getChildOf(byte[] parentHash) {
        return fileSystem.getSnapshot(fsSnapshot -> {
            return BlockTable.openReadOnly(fsSnapshot).findByParentBlockHash(parentHash).stream().map(BlockEntityConverter::fromBlockEntity).toList();
        });
    }

    @Override
    public List<? extends BlockInfo> getMetadataForBlockNumber(long blockNumber) {
        return fileSystem.getSnapshot(fsSnapshot -> {
            return BlockTable.openReadOnly(fsSnapshot).findByBlockNumber(blockNumber).stream().map(BlockEntityConverter::fromBlockEntity).toList();
        });
    }
}
