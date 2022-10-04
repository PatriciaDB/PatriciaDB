package io.patriciadb;

import io.patriciadb.core.PatriciaDBImp;
import io.patriciadb.core.transactionstable.TransactionTable;
import io.patriciadb.fs.PatriciaFileSystem;
import io.patriciadb.fs.PatriciaFileSystemFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface PatriciaDB {

    static PatriciaDB createInMemory() {
        PatriciaFileSystem fileSystem = PatriciaFileSystemFactory.inMemoryFileSystem();
        TransactionTable.initialiseTable(fileSystem);
        return new PatriciaDBImp(fileSystem);
    }

    /**
     * Create a new database, failing if there is already one
     *
     * @param configuration the configuration
     * @return the new database
     */
    static PatriciaDB createNew(Map<String,String> configuration) {
        PatriciaFileSystem fileSystem = PatriciaFileSystemFactory.createFromProperties(configuration);
        TransactionTable.initialiseTable(fileSystem);
        return new PatriciaDBImp(fileSystem);
    }

    /**
     * Open an existing database
     *
     * @param configuration the configuration
     * @return the database
     */
    static PatriciaDB open(Map<String,String> configuration) {
        PatriciaFileSystem fileSystem = PatriciaFileSystemFactory.createFromProperties(configuration);
        return new PatriciaDBImp(fileSystem);
    }

    /**
     * Read a transaction state
     *
     * @param transactionId the hash of the block
     * @return a read only snapshot of the block
     */
    ReadTransaction readTransaction(byte[] transactionId);

    /**
     * Start a new transaction starting from the state of another transaction
     *
     * @param parentTransactionId the parent transactionId
     * @return the transaction
     */
    Transaction startTransaction(byte[] parentTransactionId);

    /**
     * Start a transaction with an empty initial state.
     * This method can be used to initialise the database with the transactions
     * in the genesis block.
     *
     * @return the transaction
     */
    Transaction startTransaction(); // For the genesis block which don't have a parent block

    /**
     * Delete the block/transaction, removing all the nodes from the file system that are not referenced anymore.
     * <p>
     * This method safely deletes only the data only referenced in this transaction, and keeping
     * all the data still used by the prev and next one in the chain.
     * <p>
     * Any transaction can be deleted from the chain, either at the beginning, the end, or in the middle,
     * however there is a limitation. This method cannot be invoked on a transaction that has more
     * than 2 children (it may happen when an uncle nodes is processed). Before deleting a branch transaction
     * you have to delete one of the branch first.
     * <p>
     * When deleting multiple transactions from a chain, this method performs better by starting
     * from the most recent transaction moving backwards.
     *
     * @param blockHash the blockHash/transaction to remove
     * @throws IllegalArgumentException is the transaction is not found
     * @throws IllegalStateException    if the transaction has more or equal than 2 children transactions
     */
    void deleteTransaction(byte[] blockHash);

    /**
     * Get the metadata of the transaction
     *
     * @param transactionId the transactionId
     * @return the blockInfo
     */
    Optional<TransactionInfo> getMetadata(byte[] transactionId);

    /**
     * Get the child  of the target transaction.
     *
     * @param transactionId the transactionId
     * @return a list of BlockInfo
     */
    List<? extends TransactionInfo> getChildOf(byte[] transactionId);

    /**
     * Get list of transactions with the requested blockNumber.
     * Multiple transactions can have the same blockNumber as they
     * can be uncle or dead branches.
     *
     * @param blockNumber the blockNumber
     * @return A list of BlockInfo
     */
    List<? extends TransactionInfo> getMetadataForBlockNumber(long blockNumber);

    void vacuumFull();

    void close();

}
