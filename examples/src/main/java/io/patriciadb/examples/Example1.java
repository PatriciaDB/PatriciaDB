package io.patriciadb.examples;

import io.patriciadb.PatriciaDB;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.HexFormat;

public class Example1 {


    public static void main(String[] args) {
        Security.addProvider(new BouncyCastleProvider());

        var formatter = HexFormat.of().withLowerCase().withDelimiter(":");
        byte[] stateStorageId = "state-storage".getBytes();
        byte[] address = {89, 65, 12, 79, 123, 94, 54, 23};
        byte[] value = {13, 5, 9};

        byte[] genesisBlockHash = {87,21,90,12,53};

        PatriciaDB db = PatriciaDB.createInMemory();

        var genesisTransaction = db.startTransaction();
        try {
            var stateStorageTrie = genesisTransaction.createOrOpenStorage(stateStorageId);
            stateStorageTrie.put(address, value);
            byte[] stateStorageRootHash = stateStorageTrie.rootHash();
            System.out.printf("Genesis storage-state root hash: %s%n", formatter.formatHex(stateStorageRootHash));
            genesisTransaction.commit(genesisBlockHash);
        } finally {
            genesisTransaction.release();
        }

        byte[] block1Hash = {89,95,12,0,15,78};


        var block1transaction = db.startTransaction(genesisBlockHash);
        byte[] newValue = {13, 5, 9};

        try {
            var stateStorageTrie = block1transaction.createOrOpenStorage(stateStorageId);
            stateStorageTrie.put(address, newValue);
            byte[] stateStorageRootHash = stateStorageTrie.rootHash();

            System.out.printf("Genesis storage-state root hash at block1: %s%n", formatter.formatHex(stateStorageRootHash));

            block1transaction.commit(block1Hash);
        } finally {
            block1transaction.release();
        }

    }
}
