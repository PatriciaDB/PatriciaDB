package io.patriciadb;

public interface Transaction extends ReadTransaction, Releasable {

    /**
     * Open a Patricia Merkle Trie storage, throwing an exception if it doesn't exist
     *
     * @param storageId the storage Id
     * @return An existing storage
     */
    Storage openStorage(byte[] storageId);

    /**
     * Create a new storage, failing if it already exists
     *
     * @param storageId the storage name
     * @return A new storage
     */
    Storage createStorage(byte[] storageId);

    /**
     * Create or open an existing storage
     *
     * @param storageId the name of the storage
     * @return the storage
     */
    Storage createOrOpenStorage(byte[] storageId);

    /**
     * Commit all the changes made to all the storages
     *
     * @param blockHash the new blockHash which contained the transactions
     * @throws io.patriciadb.table.UniqueConstrainViolation if blockHash already exists
     */
    void commit(byte[] blockHash);

    /**
     * @param blockHash the new blockHash which contained the transactions
     * @param blockId   the blockId
     * @param extra     extra payload
     * @throws io.patriciadb.table.UniqueConstrainViolation if blockHash already exists
     */
    void commit(byte[] blockHash, long blockId, byte[] extra);
}
