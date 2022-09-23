package io.patriciadb.index.patriciamerkletrie;

import io.patriciadb.index.patriciamerkletrie.format.eth.EthFormat;
import io.patriciadb.index.patriciamerkletrie.utils.*;
import io.patriciadb.index.utils.InMemoryStorage;
import io.patriciadb.index.utils.KeyValue;
import io.patriciadb.index.utils.KeyValueGenerator;
import io.patriciadb.index.utils.LengthRange;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.Security;
import java.util.HexFormat;
import java.util.function.Predicate;

public class PatriciaMerkleTrieEthUnitTest {

    private final HexFormat hex = HexFormat.of();

    @BeforeAll
    public static void addProvider() {
        Security.addProvider(new BouncyCastleProvider());
    }


    @Test
    public void testDelete() {
        InMemoryStorage storage = new InMemoryStorage();

        var trie = PatriciaMerkleTrie.createNew(new EthFormat());

        var entrySource = new KeyValueGenerator(1, 1000_000, LengthRange.ofRange(8, 16), LengthRange.ofRange(15, 30));

        entrySource.stream().forEach(kv -> trie.put(kv.key(), kv.value()));

        Predicate<KeyValue> filterToDelete = kv -> kv.hashCode() % 2 == 0;

        var rootId = trie.persist(storage);



        var openTrie = PatriciaMerkleTrie.open(new EthFormat(), rootId, storage);

        entrySource.stream().filter(filterToDelete).forEach(kv -> openTrie.delete(kv.key()));


        entrySource.stream().filter(filterToDelete).forEach(kv -> Assertions.assertNull(openTrie.get(kv.key())));

        entrySource.stream().filter(filterToDelete.negate()).forEach(kv -> Assertions.assertArrayEquals(kv.value(), openTrie.get(kv.key())));

        var expectedRootHash = hex.parseHex("0020c96e8d763abcd279ca42cc30e3aab273b16c877e2cdf34844f1d752bbd55");
        var rootHash = openTrie.getRootHash();
        Assertions.assertArrayEquals(expectedRootHash, rootHash);

    }
}
