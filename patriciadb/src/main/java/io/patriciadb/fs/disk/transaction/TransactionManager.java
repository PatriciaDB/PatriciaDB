package io.patriciadb.fs.disk.transaction;

import io.patriciadb.fs.FsReadTransaction;
import io.patriciadb.fs.FsWriteTransaction;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.directory.DiskDirectory;
import io.patriciadb.utils.finalizer.Finalizer;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager implements PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(TransactionManager.class);
    private final DataStorage dataStorage;
    private final DiskDirectory directory;
    private final Finalizer finalizer;
    private final Map<Long, TransactionSession> activeTransactions = new ConcurrentHashMap<>();
    private final AtomicLong transactionCounter = new AtomicLong(1);
    private final FreeBlockIdStore freeBlockIdStore;
    private final TransactionHandler transactionHandler = new TransactionHandlerImp();

    public TransactionManager(DataStorage dataStorage, DiskDirectory directory, Finalizer finalizer) {
        this.dataStorage = dataStorage;
        this.directory = directory;
        this.finalizer = finalizer;
        Roaring64NavigableMap takenMap = new Roaring64NavigableMap(false);
        directory.forEach((blockId, pointer) -> {
            if (pointer != 0) takenMap.add(blockId);
        });
        freeBlockIdStore = new FreeBlockIdStoreImp(takenMap);
    }

    @Override
    public void destroy()  {
        activeTransactions.values().forEach(tr -> tr.setStatus(TransactionStatus.ROLLED_BACK));
    }

    public synchronized FsWriteTransaction startWriteTransaction() {
        var txId = transactionCounter.getAndIncrement();
        var transaction = new TransactionSession(transactionHandler, txId, dataStorage, directory, freeBlockIdStore);
        activeTransactions.put(txId, transaction);
        var session = transaction.createWriteSession();
        finalizer.register(session, () -> releaseInternal(transaction));
        log.debug("Starting write transaction with Id {}", txId);
        return session;
    }

    public synchronized FsReadTransaction startReadTransaction() {
        var txId = transactionCounter.getAndIncrement();
        var transaction = new TransactionSession(transactionHandler, txId, dataStorage, directory, freeBlockIdStore);
        activeTransactions.put(txId, transaction);
        var session = transaction.createReadSession();
        finalizer.register(session, () -> releaseInternal(transaction));
        log.debug("Starting read transaction with Id {}", txId);
        return session;
    }

    private void commitInternal(TransactionSession transaction) {
        if (transaction.getStatus() != TransactionStatus.RUNNING) {
            throw new IllegalStateException("Illegal state " + transaction.getStatus());
        }
        if (!activeTransactions.containsKey(transaction.getId())) {
            throw new IllegalStateException("Not an active transaction");
        }
        activeTransactions.remove(transaction.getId());
        try {

            dataStorage.flushAndSync();

            synchronized (this) {
                var changes = transaction.getChanges();
                if (activeTransactions.size() > 0) {
                    var prevDelta = directory.get(changes.keysView());
                    for (var activeTx : activeTransactions.values()) {
                        activeTx.addDeltaChange(prevDelta);
                    }
                }
                directory.set(changes);
            }
            directory.sync();


            transaction.setStatus(TransactionStatus.COMMITTED);
        } catch (Throwable t) {
            transaction.setStatus(TransactionStatus.FAILURE);
            log.error("Transaction failed", t);
        } finally {
            releaseInternal(transaction);
        }
    }

    private synchronized void releaseInternal(TransactionSession transaction) {
        if (activeTransactions.containsKey(transaction.getId())) {
            log.info("Transaction {} not released manually, using finalizer", transaction.getId());
            activeTransactions.remove(transaction.getId());
        }
        log.debug("Transaction {} terminated with status {}", transaction.getId(), transaction.getStatus());
        if (transaction.getStatus() == TransactionStatus.RUNNING) {
            transaction.setStatus(TransactionStatus.ROLLED_BACK);
            // let's re-use the taken blockId for the next transactions
            var newBlockIds = transaction.getNewBlockIds();
            if (newBlockIds.getLongCardinality() > 0) {
                log.trace("{} blockIds recovered from uncommitted transaction", newBlockIds.getLongCardinality());
                freeBlockIdStore.addFreeBlockIds(newBlockIds);
            }
        }

    }

    private class TransactionHandlerImp implements TransactionHandler {
        public void commit(TransactionSession transaction) {
            commitInternal(transaction);
        }

        public void release(TransactionSession handler) {
            if (!activeTransactions.containsKey(handler.getId())) {
                return;
            }
            activeTransactions.remove(handler.getId());
            releaseInternal(handler);
        }

    }


}
