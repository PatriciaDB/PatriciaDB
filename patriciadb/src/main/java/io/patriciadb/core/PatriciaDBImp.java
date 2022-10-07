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
            var transactionTable = TransactionTable.openReadOnly(snapshot);
            var transactionEntity = transactionTable.findByBlockHash(blockHash);
            if (transactionEntity.isEmpty()) {
                throw new IllegalArgumentException("BlockHash not found " + Arrays.toString(blockHash));
            }
            return new ReadTransactionImp(snapshot, transactionTable, transactionEntity.get());
        } catch (Throwable t) {
            snapshot.release();
            throw ExceptionUtils.sneakyThrow(t);
        }
    }

    @Override
    public Transaction startTransaction(byte[] parentBlockHash) {
        var tr = fileSystem.startTransaction();
        try {
            var blockTable = TransactionTable.open(tr);
            var parentTransactionEntity = blockTable.findByBlockHash(parentBlockHash);
            if (parentTransactionEntity.isEmpty()) {
                throw new IllegalArgumentException("BlockHash not found " + Arrays.toString(parentBlockHash));
            }
            return new TransactionImp(tr, blockTable, parentTransactionEntity.get());
        } catch (Throwable t) {
            tr.release();
            throw ExceptionUtils.sneakyThrow(t);
        }
    }

    @Override
    public Transaction startTransaction() {
        var tr = fileSystem.startTransaction();
        try {
            var transactionTable = TransactionTable.open(tr);
            var parentTransaction = new TransactionEntity();
            parentTransaction.setExtra(new byte[0]);
            parentTransaction.setCreationTime(Instant.now());
            parentTransaction.setTransactionId(new byte[0]);
            parentTransaction.setParentTransactionId(new byte[0]);
            parentTransaction.setIndexRootNodeId(0);
            return new TransactionImp(tr, transactionTable, parentTransaction);
        } catch (Throwable t) {
            tr.release();
            throw ExceptionUtils.sneakyThrow(t);
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
