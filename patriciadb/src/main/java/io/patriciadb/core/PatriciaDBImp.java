package io.patriciadb.core;

import io.patriciadb.*;
import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.core.transactionstable.TransactionTable;
import io.patriciadb.fs.PatriciaFileSystem;
import io.patriciadb.core.utils.TransactionInfoMapper;
import io.patriciadb.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class PatriciaDBImp implements PatriciaDB {

    private final static Logger log = LoggerFactory.getLogger(PatriciaDBImp.class);

    private final PatriciaFileSystem fileSystem;
    private final TransactionDeleteController transactionDeleteController;

    public PatriciaDBImp(PatriciaFileSystem fileSystem) {
        this.fileSystem = fileSystem;
        this.transactionDeleteController = new TransactionDeleteController(fileSystem);
        validate();
    }

    private void validate() {
        var tr = fileSystem.getSnapshot();
        try {
            var table = TransactionTable.openReadOnly(tr);
        } finally {
            tr.release();
        }
    }

    @Override
    public ReadTransaction readTransaction(byte[] blockHash) {
        var snapshot = fileSystem.getSnapshot();
        try {
            var blockTable = TransactionTable.openReadOnly(snapshot);
            var blockInfo = blockTable.findByBlockHash(blockHash);
            if (blockInfo.isEmpty()) {
                throw new IllegalArgumentException("BlockHash not found " + Arrays.toString(blockHash));
            }
            return new ReadTransactionImp(snapshot, blockInfo.get());
        } catch (Throwable t) {
            snapshot.release();
            throw ExceptionUtils.sneakyThrow(t);
        }
    }

    @Override
    public Transaction startTransaction(byte[] parentBlockHash) {
        var tr = fileSystem.getSnapshot();
        try {
            var blockTable = TransactionTable.openReadOnly(tr);
            var parentBlockInfo = blockTable.findByBlockHash(parentBlockHash);
            if (parentBlockInfo.isEmpty()) {
                throw new IllegalArgumentException("BlockHash not found " + Arrays.toString(parentBlockHash));
            }
            return new TransactionImp(fileSystem, parentBlockInfo.get());
        } finally {
            tr.release();
        }
    }

    @Override
    public Transaction startTransaction() {
        var tr = fileSystem.startTransaction();
        try {
            var parentBlock = new TransactionEntity();
            parentBlock.setExtra(new byte[0]);
            parentBlock.setCreationTime(Instant.now());
            parentBlock.setTransactionId(new byte[0]);
            parentBlock.setParentTransactionId(new byte[0]);
            parentBlock.setIndexRootNodeId(0);
            return new TransactionImp(fileSystem, parentBlock);
        } finally {
            tr.release();
        }
    }

    @Override
    public void deleteTransaction(byte[] blockHash) {
        transactionDeleteController.deleteTransaction(blockHash);
    }

    @Override
    public Optional<TransactionInfo> getMetadata(byte[] transactionId) {
        return fileSystem.getSnapshot(fsSnapshot -> {
            return TransactionTable.openReadOnly(fsSnapshot).findByBlockHash(transactionId).map(TransactionInfoMapper::fromBlockEntity);
        });
    }

    @Override
    public List<? extends TransactionInfo> getChildOf(byte[] parentHash) {
        return fileSystem.getSnapshot(fsSnapshot -> {
            return TransactionTable.openReadOnly(fsSnapshot).findByParentBlockHash(parentHash).stream().map(TransactionInfoMapper::fromBlockEntity).toList();
        });
    }

    @Override
    public List<? extends TransactionInfo> getMetadataForBlockNumber(long blockNumber) {
        return fileSystem.getSnapshot(fsSnapshot -> {
            return TransactionTable.openReadOnly(fsSnapshot).findByBlockNumber(blockNumber).stream().map(TransactionInfoMapper::fromBlockEntity).toList();
        });
    }

    @Override
    public void vacuumFull() {
        fileSystem.runVacuum();
    }

    @Override
    public void close() {
        fileSystem.close();
    }
}
