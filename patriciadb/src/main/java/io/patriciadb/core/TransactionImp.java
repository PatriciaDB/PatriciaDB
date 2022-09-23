package io.patriciadb.core;

import io.patriciadb.Storage;
import io.patriciadb.Transaction;
import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.core.blocktable.BlockTable;
import io.patriciadb.fs.FSTransaction;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionImp implements Transaction {
    private final static Logger log = LoggerFactory.getLogger(TransactionImp.class);
    private final FSTransaction transaction;
    private final BlockTable blockTable;
    private final BlockEntity parentEntity;
    private final PatriciaMerkleTrie storageIndex;
    private final ConcurrentHashMap<Bytes, StorageImp> tries = new ConcurrentHashMap<>();
    private final PersistedNodeObserverTracker persistedNodeObserverTracker = new PersistedNodeObserverTracker();

    public TransactionImp(FSTransaction transaction, BlockTable blockTable, BlockEntity parentEntity) {
        this.transaction = transaction;
        this.blockTable = blockTable;
        this.parentEntity = parentEntity;
        this.storageIndex = PatriciaMerkleTrie.openOrCreate(Formats.ETHEREUM, parentEntity.getIndexRootNodeId(), transaction, persistedNodeObserverTracker);
    }

    @Override
    public void release() {
        transaction.release();
    }

    @Override
    public synchronized Storage openStorage(byte[] storageId) {
        return tries.computeIfAbsent(Bytes.wrap(storageId), k -> {
            var res = storageIndex.get(storageId);
            if (res == null) {
                throw new RuntimeException("Storage not found");
            }
            var trie = PatriciaMerkleTrie.open(Formats.ETHEREUM, VarInt.getVarLong(ByteBuffer.wrap(res)), transaction, persistedNodeObserverTracker);
            return new StorageImp(trie);
        });
    }

    @Override
    public synchronized Storage createStorage(byte[] storageId) {
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

    @Override
    public synchronized void commit(byte[] blockHash) {
        for (var entry : tries.entrySet()) {
            var name = entry.getKey();
            var storage = entry.getValue();

            storage.trie.getRootHash();
            long rootId = storage.trie.persist(transaction);
            storageIndex.put(name.getBytes(), VarInt.varLong(rootId));
            log.debug("Persisted storage {} rootId {}", name,rootId);
        }
        long storageIndexRootId = storageIndex.persist(transaction);

        var newNodes = persistedNodeObserverTracker.getNewNodes();
        var lostNodes = persistedNodeObserverTracker.getLostNodes();

        log.debug("Commit Prepare - New nodes count {}, Lost Nodes Reference {}", newNodes.getLongCardinality(), lostNodes.getLongCardinality());
        BlockEntity blockEntity = new BlockEntity();
        blockEntity.setBlockHash(blockHash);
        blockEntity.setBlockNumber(parentEntity.getBlockNumber() + 1);
        blockEntity.setCreationTime(Instant.now());
        blockEntity.setParentBlockHash(parentEntity.getBlockHash());
        blockEntity.setIndexRootNodeId(storageIndexRootId);
        blockEntity.setExtra("");
        blockEntity.setNewNodeIds(BitMapUtils.serialize(newNodes));
        blockEntity.setLostNodeIds(BitMapUtils.serialize(lostNodes));
        blockTable.insert(blockEntity);
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
