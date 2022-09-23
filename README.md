# PatriciaDB

PatriciaDB is a new database under development based on a Patricia Merkle Trie index written in Java.
Initially available only as a library (embedded), it will be available as a server application with client library
written for different languages.

Can be used by Ethereum full node clients or by other blockchains.

For more information read our wiki:
https://github.com/PatriciaDB/PatriciaDB/wiki

# Features
PatriciaDB is new database currently under development offering ACID properties.
* Fully atomic transactions
* Snapshot read
* Automatically keeps tracks of all the past transaction and blocks
* On each transaction you can handle an unlimited number of storage (of Patricia Merkle Trie)
* You can purge any transaction from the history


# Performance and space used
Early benchmarks suggests that PatriciaDB is between 5 to 10 times faster than the implementation provided by the Besu client (PatriciaMerkleTrie + RocksDB),
and it uses half of the space.

# Example
At this stage PatriciaDB can only be tested as a library in embedded mode.

```java
// Add BouncyCastle as a security provider to support Keccak-256
Security.addProvider(new BouncyCastleProvider());

// We need to create a PatriciaFileSystem
var databaseFolder = ...;
var fs = new DiskFileSystem(databaseFolder);

var patricDB = PatriciaDB.createNew(fs);

// We write the genesis block to the database
var genesisBlockTransaction = patricDB.startTransaction();
try {
    var stateStorage = genesisBlockTransaction.createOrOpenStorage("state".getBytes());
    stateStorage.put("hello".getBytes(), "world".getBytes());
    
    var philipStorage = genesisBlockTransaction.createOrOpenStorage("philipAccount".getBytes());
    philipStorage.put("hello".getBytes(), "Philip".getBytes());
    // We get the rootHash of this storage in Ethereum format
    byte[] philipRootHash = philipStorage.rootHash();
    
    // We you make a commit you must give to the transaction a unique identifier
    // which can be the blockHash
    genesis.commit("block0hash".getBytes());
} finally {
    // Alway release a transaction to avoid memory leak
    genesisBlockTransaction.release();
}

// Next transaction start from the genesis block hash.
// You don't have to start from the tail, you can create a new transaction from any block
var block0Transaction = patricDB.startTransaction("block0hash".getBytes());
try {
    var stateStorage = block0Transaction.createOrOpenStorage("state".getBytes());
    System.out.println(new String(state.get("hello".getBytes()))); // print "world"
    
    var philipStorage = block0Transaction.createOrOpenStorage("philipAccount".getBytes());
    System.out.println(new String(philipStorage.get("hello".getBytes()))); // print "philip"
    // We make a commit using the block hash
    genesis.commit("block1hash".getBytes());
} finally {
    block0Transaction.release();
}
```

# Contribute
There are may ways you can contribute in the future, however at this stage of the project we can only accept feedback.
and no pull requests.

# Contacts
Join our discord server: https://discord.gg/3FG9GgHzsw
