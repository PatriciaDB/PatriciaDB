package io.patriciadb;

import io.patriciadb.core.TransactionDeleteController;
import io.patriciadb.index.patriciamerkletrie.PatriciaMerkleTrie;
import io.patriciadb.index.patriciamerkletrie.format.Formats;
import io.patriciadb.index.patriciamerkletrie.utils.PersistedNodeObserverTracker;
import io.patriciadb.index.patriciamerkletrie.utils.TrieDeltaChanges;
import io.patriciadb.index.utils.InMemoryStorage;
import io.patriciadb.index.utils.KeyValueGenerator;
import io.patriciadb.index.utils.LengthRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionDeleteLogicUnitTest {


    @Test
    public void purgeUnitTest() {

        var storage = new InMemoryStorage();

        var origin = new TransactionHistory(0, null, new TrieDeltaChanges(new Roaring64NavigableMap(false), new Roaring64NavigableMap(false)));

        var t1 = insert(storage, origin, 1, 100); // 100 keys
        var t2 = insert(storage, t1, 2, 100); // 200 keys
        var t3 = delete(storage, t2, 1, 50); // 150 keys
        var t4 = insert(storage, t3, 3, 50); // 200 keys
        var t5 = insert(storage, t4, 4, 50); // 250 keys
        var t6 = insert(storage, t5, 5, 50); // 300 keys
        var uncle = insert(storage, t3, 7, 60); // 210 keys
        System.out.println("Child node count "+storage.nodeCount());

        Assertions.assertEquals(100, countKeys(storage, t1));
        Assertions.assertEquals(200, countKeys(storage, t2));
        Assertions.assertEquals(150, countKeys(storage, t3));
        Assertions.assertEquals(200, countKeys(storage, t4));
        Assertions.assertEquals(250, countKeys(storage, t5));
        Assertions.assertEquals(300, countKeys(storage, t6));
        Assertions.assertEquals(210, countKeys(storage, uncle));

        var toDelete = delete(t2);
        toDelete.forEach(storage::delete);

        Assertions.assertEquals(100, countKeys(storage, t1));
        Assertions.assertEquals(150, countKeys(storage, t3));
        Assertions.assertEquals(200, countKeys(storage, t4));
        Assertions.assertEquals(250, countKeys(storage, t5));
        Assertions.assertEquals(300, countKeys(storage, t6));
        Assertions.assertEquals(210, countKeys(storage, uncle));

        toDelete = delete(uncle);
        toDelete.forEach(storage::delete);


        Assertions.assertEquals(100, countKeys(storage, t1));
        Assertions.assertEquals(150, countKeys(storage, t3));
        Assertions.assertEquals(200, countKeys(storage, t4));
        Assertions.assertEquals(250, countKeys(storage, t5));
        Assertions.assertEquals(300, countKeys(storage, t6));

        toDelete = delete(t4);
        toDelete.forEach(storage::delete);


        Assertions.assertEquals(100, countKeys(storage, t1));
        Assertions.assertEquals(150, countKeys(storage, t3));
        Assertions.assertEquals(250, countKeys(storage, t5));
        Assertions.assertEquals(300, countKeys(storage, t6));

        toDelete = delete(t6);
        toDelete.forEach(storage::delete);


        Assertions.assertEquals(100, countKeys(storage, t1));
        Assertions.assertEquals(150, countKeys(storage, t3));
        Assertions.assertEquals(250, countKeys(storage, t5));

        toDelete = delete(t3);
        toDelete.forEach(storage::delete);


        Assertions.assertEquals(100, countKeys(storage, t1));
        Assertions.assertEquals(250, countKeys(storage, t5));

        toDelete = delete(t1);
        toDelete.forEach(storage::delete);

        Assertions.assertEquals(250, countKeys(storage, t5));

        toDelete = delete(t5);
        toDelete.forEach(storage::delete);

        System.out.println("Node count "+storage.nodeCount());

        Assertions.assertEquals(0, storage.nodeCount());

    }

    public static TransactionHistory insert(InMemoryStorage storage, TransactionHistory prev, int seed, int count) {
        var nodeTracker = new PersistedNodeObserverTracker();
        var trie = PatriciaMerkleTrie.openOrCreate(Formats.PLAIN, prev.rootId, storage, nodeTracker);
        KeyValueGenerator generator = new KeyValueGenerator(seed, count, LengthRange.ofSize(32), LengthRange.ofSize(32));

        generator.stream().forEach(kv -> trie.put(kv.key(), kv.value()));

        var newRootId = trie.persist(storage);
        TransactionHistory transactionHistory = new TransactionHistory(newRootId, prev, nodeTracker.toTrieDeltaChange());
        prev.next.add(transactionHistory);
        return transactionHistory;
    }

    public static TransactionHistory delete(InMemoryStorage storage, TransactionHistory prev, int seed, int count) {
        var nodeTracker = new PersistedNodeObserverTracker();
        var trie = PatriciaMerkleTrie.openOrCreate(Formats.PLAIN, prev.rootId, storage, nodeTracker);
        KeyValueGenerator generator = new KeyValueGenerator(seed, count, LengthRange.ofSize(32), LengthRange.ofSize(32));

        generator.stream().forEach(kv -> trie.delete(kv.key()));

        var newRootId = trie.persist(storage);
        TransactionHistory transactionHistory = new TransactionHistory(newRootId, prev, nodeTracker.toTrieDeltaChange());
        prev.next.add(transactionHistory);
        return transactionHistory;
    }

    public static long countKeys(InMemoryStorage storage, TransactionHistory tr) {
        var trie = PatriciaMerkleTrie.openOrCreate(Formats.PLAIN, tr.rootId, storage);
        AtomicLong counter = new AtomicLong(0);
        trie.forEach((k, v) -> counter.incrementAndGet());
        return counter.get();
    }

    private static class TransactionHistory {
        private final long rootId;
        private TransactionHistory parent;
        private final TrieDeltaChanges trieDeltaChanges;
        private final List<TransactionHistory> next = new ArrayList<>();

        public TransactionHistory(long rootId, TransactionHistory parent, TrieDeltaChanges trieDeltaChanges) {
            this.rootId = rootId;
            this.parent = parent;
            this.trieDeltaChanges = trieDeltaChanges;
        }

        public long getRootId() {
            return rootId;
        }

        public TrieDeltaChanges getTrieDeltaChanges() {
            return trieDeltaChanges;
        }
    }


    public static Roaring64NavigableMap delete(TransactionHistory tr) {
        if(tr.next.size()>1) {
            throw new IllegalStateException("Cannot delete branch node");
        }
        var toDelete = TransactionDeleteController.getDeletableNodes(tr.trieDeltaChanges, tr.next.stream().map(TransactionHistory::getTrieDeltaChanges).findFirst().orElse(null));
        if(!tr.next.isEmpty()) {
            TransactionDeleteController.updateChildDeltaChanges(tr.trieDeltaChanges, tr.next.get(0).trieDeltaChanges);
        }
        for (var child : tr.next) {
            child.parent = tr.parent;
        }
        tr.parent.next.remove(tr);
        tr.parent.next.addAll(tr.next);
        return toDelete;
    }


}
