package io.patriciadb.core;

import io.patriciadb.core.blocktable.BlockTable;
import io.patriciadb.fs.FSTransaction;
import io.patriciadb.utils.BitMapUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class BlockPurger {
    private final static Logger log = LoggerFactory.getLogger(BlockPurger.class);

    public static boolean purgeBlock(FSTransaction transaction, byte[] blockHash) {
        var blockTable = BlockTable.open(transaction);
        var blockOpt = blockTable.findByBlockHash(blockHash);
        if (blockOpt.isEmpty()) {
            throw new IllegalArgumentException("Block not found");
        }
        log.debug("Deleting transaction/block with hash {}", Arrays.toString(blockHash));
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
        nodeToDelete.forEach(transaction::delete);

        for (var childBlock : childBlocks) {
            blockTable.update(childBlock);
        }
        blockTable.delete(block.getPrimaryKey());
        return true;
    }
}
