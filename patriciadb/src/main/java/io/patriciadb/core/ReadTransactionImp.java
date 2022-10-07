package io.patriciadb.core;

import io.patriciadb.ReadTransaction;
import io.patriciadb.StorageNotFoundException;
import io.patriciadb.StorageRead;
import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.core.transactionstable.TransactionTableRead;
import io.patriciadb.fs.FsReadTransaction;
import io.patriciadb.index.patriciamerkletrie.PatriciaMerkleTrie;
import io.patriciadb.index.patriciamerkletrie.format.Formats;
import io.patriciadb.utils.Bytes;
import io.patriciadb.utils.VarInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class ReadTransactionImp implements ReadTransaction {
    private final static Logger log = LoggerFactory.getLogger(ReadTransactionImp.class);
    private final FsReadTransaction snapshot;
    private final TransactionTableRead blockTable;
    private final TransactionEntity transactionEntity;
    private final PatriciaMerkleTrie storageIndex;
    private final ConcurrentHashMap<Bytes, StorageReadImp> tries = new ConcurrentHashMap<>();

    public ReadTransactionImp(FsReadTransaction snapshot, TransactionTableRead blockTable, TransactionEntity transactionEntity) {
        this.snapshot = snapshot;
        this.blockTable = blockTable;
        this.transactionEntity = transactionEntity;
        this.storageIndex = PatriciaMerkleTrie.open(Formats.PLAIN, transactionEntity.getIndexRootNodeId(), snapshot);
    }


    @Override
    public StorageRead openStorage(byte[] storageId) {
        return tries.computeIfAbsent(Bytes.wrap(storageId), k -> {
            var res = storageIndex.get(storageId);
            if (res == null) {
                throw new StorageNotFoundException("Storage "+ Arrays.toString(storageId)+" not found");
            }
            var trie = PatriciaMerkleTrie.open(Formats.ETHEREUM, VarInt.getVarLong(ByteBuffer.wrap(res)), snapshot);
            return new StorageReadImp(trie);
        });
    }

    @Override
    public void release() {
        snapshot.release();
    }

    private class StorageReadImp implements StorageRead {
        private final PatriciaMerkleTrie trie;

        public StorageReadImp(PatriciaMerkleTrie trie) {
            this.trie = trie;
        }

        @Override
        public byte[] get(byte[] key) {
            return trie.get(key);
        }

        @Override
        public byte[] rootHash() {
            return trie.getRootHash();
        }
    }
}
