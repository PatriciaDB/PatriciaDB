package io.patriciadb.core;

import io.patriciadb.Storage;
import io.patriciadb.StorageNotFoundException;
import io.patriciadb.Transaction;
import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.core.transactionstable.TransactionTable;
import io.patriciadb.fs.FsWriteTransaction;
import io.patriciadb.index.patriciamerkletrie.PatriciaMerkleTrie;
import io.patriciadb.index.patriciamerkletrie.format.Formats;
import io.patriciadb.index.patriciamerkletrie.utils.PersistedNodeObserverTracker;
import io.patriciadb.utils.BitMapUtils;
import io.patriciadb.utils.Bytes;
import io.patriciadb.utils.VarInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionImp implements Transaction {
    private final static Logger log = LoggerFactory.getLogger(TransactionImp.class);
    private final FsWriteTransaction transaction;
    private final TransactionTable blockTable;
    private final TransactionEntity parentEntity;
    private final PatriciaMerkleTrie storageIndex;
    private final ConcurrentHashMap<Bytes, StorageImp> tries = new ConcurrentHashMap<>();
    private final PersistedNodeObserverTracker persistedNodeObserverTracker = new PersistedNodeObserverTracker();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    public TransactionImp(FsWriteTransaction transaction, TransactionTable blockTable, TransactionEntity parentEntity) {
        this.transaction = transaction;
        this.blockTable = blockTable;
        this.parentEntity = parentEntity;
        this.storageIndex = PatriciaMerkleTrie.openOrCreate(Formats.PLAIN, parentEntity.getIndexRootNodeId(), transaction, persistedNodeObserverTracker);
    }

    @Override
    public void release() {
        transaction.release();
    }

    @Override
    public synchronized Storage openStorage(byte[] storageId) {
        checkState();
        return tries.computeIfAbsent(Bytes.wrap(storageId), k -> {
            var res = storageIndex.get(storageId);
            if (res == null) {
                throw new StorageNotFoundException("Storage " + Arrays.toString(storageId) + " not found");
            }
            var trie = PatriciaMerkleTrie.open(Formats.ETHEREUM, VarInt.getVarLong(ByteBuffer.wrap(res)), transaction, persistedNodeObserverTracker);
            return new StorageImp(trie);
        });
    }

    @Override
    public synchronized Storage createStorage(byte[] storageId) {
        checkState();
        return tries.computeIfAbsent(Bytes.wrap(storageId), k -> {
            var res = storageIndex.get(storageId);
            PatriciaMerkleTrie trie;
            if (res == null) {
                trie = PatriciaMerkleTrie.createNew(Formats.ETHEREUM, persistedNodeObserverTracker);
            } else {
                throw new RuntimeException("Storage already exists");
            }
            return new StorageImp(trie);
        });
    }

    @Override
    public synchronized Storage createOrOpenStorage(byte[] storageId) {
        checkState();
        return tries.computeIfAbsent(Bytes.wrap(storageId), k -> {
            var res = storageIndex.get(storageId);
            PatriciaMerkleTrie trie;
            if (res == null) {
                trie = PatriciaMerkleTrie.createNew(Formats.ETHEREUM, persistedNodeObserverTracker);
            } else {
                trie = PatriciaMerkleTrie.open(Formats.ETHEREUM, VarInt.getVarLong(ByteBuffer.wrap(res)), transaction, persistedNodeObserverTracker);
            }
            return new StorageImp(trie);
        });
    }

    private void checkState() {
        if (disposed.get()) {
            throw new IllegalStateException("This transaction has been disposed");
        }
    }

    @Override
    public synchronized void commit(byte[] blockHash) {
        checkState();
        disposed.set(true);
        commitInternal(blockHash, parentEntity.getBlockNumber() + 1, new byte[0]);
    }

    @Override
    public void commit(byte[] blockHash, long blockId, byte[] extra) {
        checkState();
        disposed.set(true);
        commitInternal(blockHash, blockId, extra);
    }

    private void commitInternal(byte[] blockHash, long blockId, byte[] extra) {
        Objects.requireNonNull(blockHash, "BlockHash cannot be null");
        if (extra == null) extra = new byte[0];
        if (extra.length > 2048) {
            throw new IllegalArgumentException("Extra data cannot be longer than 2048 bytes");
        }
        for (var entry : tries.entrySet()) {
            var name = entry.getKey();
            var storage = entry.getValue();

            storage.trie.getRootHash();
            long rootId = storage.trie.persist(transaction);
            storageIndex.put(name.getBytes(), VarInt.varLong(rootId));
            log.debug("Persisted storage {} rootId {}", name, rootId);
        }
        long storageIndexRootId = storageIndex.persist(transaction);

        var newNodes = persistedNodeObserverTracker.getNewNodes();
        var lostNodes = persistedNodeObserverTracker.getLostNodes();

        log.debug("Commit Prepare - New nodes count {}, Lost Nodes Reference {}", newNodes.getLongCardinality(), lostNodes.getLongCardinality());
        TransactionEntity transactionEntity = new TransactionEntity();
        transactionEntity.setTransactionId(blockHash);
        transactionEntity.setBlockNumber(blockId);
        transactionEntity.setCreationTime(Instant.now());
        transactionEntity.setParentTransactionId(parentEntity.getTransactionId());
        transactionEntity.setIndexRootNodeId(storageIndexRootId);
        transactionEntity.setExtra(extra);
        transactionEntity.setNewNodeIds(BitMapUtils.serialize(newNodes));
        transactionEntity.setLostNodeIds(BitMapUtils.serialize(lostNodes));
        blockTable.insert(transactionEntity);
        blockTable.persist();
        transaction.commit();
        log.debug("Commit completed");
    }

    private class StorageImp implements Storage {
        private final PatriciaMerkleTrie trie;

        public StorageImp(PatriciaMerkleTrie trie) {
            this.trie = trie;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            trie.put(key, value);
        }

        @Override
        public void delete(byte[] key) {
            trie.delete(key);
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
