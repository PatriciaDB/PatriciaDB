package io.patriciadb;

public interface Storage extends StorageRead {
    /**
     * Writes a new key to the trie, replacing the existing value.
     * @param key the key
     * @param value the value
     */
    void put(byte[] key, byte[] value);

    /**
     * Delete an existing client
     * @param key the key
     */
    void delete(byte[] key);

}
