package io.patriciadb.core;

import io.patriciadb.core.transactionstable.TransactionTable;
import io.patriciadb.fs.FSTransaction;
import io.patriciadb.fs.PatriciaFileSystem;
import io.patriciadb.index.patriciamerkletrie.utils.TrieDeltaChanges;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * Delete a transaction all the nodes of the tries that are not referenced anymore.
 */
public class TransactionDeleteController {
    private final static Logger log = LoggerFactory.getLogger(TransactionDeleteController.class);

    private final PatriciaFileSystem fileSystem;

    public TransactionDeleteController(PatriciaFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void deleteTransaction(byte[] blockHash) {
        fileSystem.startTransaction(tr -> deleteTransactionInternal(tr, blockHash));
    }

    private boolean deleteTransactionInternal(FSTransaction transaction, byte[] blockHash) {

        var blockTable = TransactionTable.open(transaction);
        var blockOpt = blockTable.findByBlockHash(blockHash);
        if (blockOpt.isEmpty()) {
            throw new IllegalArgumentException("Block not found");
        }
        log.debug("Deleting transaction/block with hash {}", Arrays.toString(blockHash));
        var block = blockOpt.get();
        var childBlocks = blockTable.findByParentBlockHash(blockHash);
        if (childBlocks.size() > 1) {
            // Branch transactions cannot be deleted with this strategy.
            throw new IllegalStateException("Cannot delete a branch transaction with more than 1 child");
        }
        var deltaChange = TrieDeltaChanges.fromBlockEntity(block);

        var childBlock = childBlocks.isEmpty() ? null : childBlocks.get(0);
        var deltaChangeChild = childBlock == null ? null : TrieDeltaChanges.fromBlockEntity(childBlock);
        var nodeToDelete = getDeletableNodes(deltaChange, deltaChangeChild);

        if (deltaChangeChild != null) {
            updateChildDeltaChanges(deltaChange, deltaChangeChild);
            deltaChangeChild.setToBlock(childBlock);
            blockTable.update(childBlock);
            log.trace("Child block Size {}", childBlock.getNewNodeIds().length+childBlock.getLostNodeIds().length);
        }

        log.debug("Deleting {} nodes from transaction history id {}", nodeToDelete.getLongCardinality(), Arrays.toString(blockHash));
        nodeToDelete.forEach(transaction::delete);

        blockTable.delete(block.getPrimaryKey());
        return true;
    }


    // Visible for testing
    public static Roaring64NavigableMap getDeletableNodes(@Nonnull TrieDeltaChanges target, @Nullable TrieDeltaChanges next) {
        var nodeToDelete = new Roaring64NavigableMap(false);
        nodeToDelete.or(target.newNodes());
        if (next == null) {
            return nodeToDelete;
        }
        nodeToDelete.and(next.lostNodes());
        return nodeToDelete;
    }

    // Visible for testing
    public static void updateChildDeltaChanges(@Nonnull TrieDeltaChanges target, @Nonnull TrieDeltaChanges next) {
        next.newNodes().or(target.newNodes());
        next.newNodes().andNot(target.lostNodes());
        next.newNodes().andNot(next.lostNodes());
        next.lostNodes().andNot(target.newNodes());
        next.lostNodes().or(target.lostNodes());
    }
}
