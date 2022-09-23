package io.patriciadb.index.btree;

import io.patriciadb.index.utils.InMemoryStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class BTreeUnitTest {
    @Test
    public void basicTest() {
        BTree<Long> btree = BTree.createNew(LongSerializer.INSTANCE);
        for (long i = 0; i < 10000; i++) {
            btree.insert(i);
        }
        List<Long> expected =  List.of(200L, 201L, 202L, 203L, 204L,205L,206L, 207L, 208L, 209L, 210L, 211L, 212L, 213L, 214L, 215L, 216L, 217L, 218L, 219L, 220L);

        var res = btree.find(KeyRange.of(200L, 220L));
        Assertions.assertEquals(expected, res);

        btree.delete(205L);
        btree.delete(206L);
        var res2 = btree.find(KeyRange.of(200L, 220L));
        List<Long> expected2 =  List.of(200L, 201L, 202L, 203L, 204L, 207L, 208L, 209L, 210L, 211L, 212L, 213L, 214L, 215L, 216L, 217L, 218L, 219L, 220L);
        Assertions.assertEquals(expected2, res2);
    }

    @Test
    public void addDeleteNodeLeakTest() {

        var inMemory = new InMemoryStorage();

        BTree<AccountIndex> newBtree = BTree.createNew(AccountIndex.SERIALIZER);
        newBtree.persistChanges(inMemory);
        long rootId = newBtree.getRootId().orElseThrow();

        LinkedList<Long> accountIds = new LinkedList<>();
        for (int batch = 0; batch < 1000; batch++) {
            BTree<AccountIndex> btree = BTree.open(rootId, AccountIndex.SERIALIZER, inMemory);
            for (int i = 0; i < 2000; i++) {
                long accountId = batch * 100000 + i;
                btree.insert(new AccountIndex(accountId, ThreadLocalRandom.current().nextLong(100)));
                accountIds.add(accountId);
            }
            btree.persistChanges(inMemory);
            rootId = btree.getRootId().orElseThrow();
        }

        System.out.println("NodeCount " + inMemory.nodeCount());
        System.out.println("TotalSize " + inMemory.totalSize());

        Collections.shuffle(accountIds);

        while (!accountIds.isEmpty()) {
            BTree<AccountIndex> btree = BTree.open(rootId, AccountIndex.SERIALIZER, inMemory);
            for (int i = 0; i < 2000 && !accountIds.isEmpty(); i++) {
                long accountId = accountIds.removeFirst();
                var account = btree.find(KeyRange.of(new AccountIndex(accountId, Long.MIN_VALUE), new AccountIndex(accountId, Long.MAX_VALUE))).get(0);
                btree.delete(account);
            }
            btree.persistChanges(inMemory);
            rootId = btree.getRootId().orElseThrow();
        }

        System.out.println("New rootId " + rootId);
        Assertions.assertEquals(1, inMemory.nodeCount());
        Assertions.assertEquals(2, inMemory.totalSize());
    }


}
