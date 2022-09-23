package io.patriciadb;

public interface StorageRead {

    /**
     * Reads a key from the Patricia Merkle trie
     * @param key the key
     * @return the value associated with this key, null of absent
     */
    byte[] get(byte[] key);


    /**
     * The root hash of this trie.
     * @return The root hash of this trie
     */
    byte[] rootHash();

}
