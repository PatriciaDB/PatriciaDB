package io.patriciadb;

import io.patriciadb.core.blocktable.BlockTable;
import io.patriciadb.core.PatriciaDBImp;
import io.patriciadb.fs.PatriciaFileSystem;

import java.util.List;
import java.util.Optional;

public interface PatriciaDB {

    /**
     * Create a new database, failing if there is already one
     * @param fileSystem the file system
     * @return the new database
     */
    public static PatriciaDB createNew(PatriciaFileSystem fileSystem) {
        BlockTable.initialiseTable(fileSystem);
        return open(fileSystem);
    }

    /**
     * Open an existing database
     * @param fileSystem the file system
     * @return the database
     */
    static PatriciaDB open(PatriciaFileSystem fileSystem) {
        return new PatriciaDBImp(fileSystem);
    }

    /**
     * Read a transaction/block state
     * @param blockHash the hash of the block
     * @return a read only snapshot of the block
     */
    Snapshot readTransaction(byte[] blockHash);

    /**
     * Start a new transaction which the starting point is the blockHash use to start a transaction
     * @param parentBlockHash the parent blockHash
     * @return the transaction
     */
    Transaction startTransaction(byte[] parentBlockHash);

    /**
     * Start a transaction without a parent block. The initial states is empty with no storages.
     * @return the transaction
     */
    Transaction startTransaction(); // For the genesis block which don't have a parent block

    /**
     * Purge the blockInfos, removing all the nodes from the file system that are not referenced anymore
     * @param blockHash the blockHash/transaction to remove
     */
    void purgeBlockData(byte[] blockHash);

    /**
     * Get the metadata informations of the transaction/block
     * @param blockHash the blockHash
     * @return the blockInfo
     */
    Optional<BlockInfo> getMetadata(byte[] blockHash);

    /**
     * Get the child metadata of block
     * @param blockHash the blockHash
     * @return a list of BlockInfo
     */
    List<? extends BlockInfo> getChildOf(byte[] blockHash);

    /**
     * Get list of blocks with the requested blockNumber. Multiple transactions can have the same blockNumber as they
     * can be uncle or dead branches.
     * @param blockNumber the blockNumber
     * @return A list of BlockInfo
     */
    List<? extends BlockInfo> getMetadataForBlockNumber(long blockNumber);

}
