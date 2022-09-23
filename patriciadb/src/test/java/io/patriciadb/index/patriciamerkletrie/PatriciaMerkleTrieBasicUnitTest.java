package io.patriciadb.index.patriciamerkletrie;

import io.patriciadb.index.utils.KeyValue;
import io.patriciadb.index.utils.KeyValueGenerator;
import io.patriciadb.index.utils.LengthRange;
import io.patriciadb.index.patriciamerkletrie.format.eth.EthFormat;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.Security;
import java.util.HexFormat;
import java.util.function.Predicate;

public class PatriciaMerkleTrieBasicUnitTest {

    private final HexFormat hex = HexFormat.of();

    @BeforeAll
    public static void addProvider() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void testInsert() {
        var trie = PatriciaMerkleTrie.createNew(new EthFormat());

        var entrySource = new KeyValueGenerator(1, 1000_000, LengthRange.ofRange(8, 16), LengthRange.ofRange(15, 30));

        entrySource.stream().forEach(kv -> trie.put(kv.key(), kv.value()));


        entrySource.stream().forEach(kv -> Assertions.assertArrayEquals(kv.value(), trie.get(kv.key())));

        // Expected hash: a3f4f27c807a461b59a02fd3b2ba54eb4d08bf3c8f55e4b2cd98f375b5ddb546
        var rootHash = trie.getRootHash();
        var expectedRootHash =hex.parseHex("a3f4f27c807a461b59a02fd3b2ba54eb4d08bf3c8f55e4b2cd98f375b5ddb546");
        Assertions.assertArrayEquals(expectedRootHash, rootHash);

    }

    @Test
    public void testDelete() {
        var trie = PatriciaMerkleTrie.createNew(new EthFormat());

        var entrySource = new KeyValueGenerator(1, 1_000_000, LengthRange.ofRange(8, 16), LengthRange.ofRange(15, 30));

        entrySource.stream().forEach(kv -> trie.put(kv.key(), kv.value()));

        Predicate<KeyValue> filterToDelete = kv -> kv.hashCode()%2==0;

        entrySource.stream().filter(filterToDelete).forEach(kv -> trie.delete(kv.key()));


        entrySource.stream().filter(filterToDelete).forEach(kv -> Assertions.assertNull(trie.get(kv.key())));

        entrySource.stream().filter(filterToDelete.negate()).forEach(kv -> Assertions.assertArrayEquals(kv.value(), trie.get(kv.key())));

        var expectedRootHash = hex.parseHex("0020c96e8d763abcd279ca42cc30e3aab273b16c877e2cdf34844f1d752bbd55");
        var rootHash = trie.getRootHash();
        Assertions.assertArrayEquals(expectedRootHash, rootHash);
    }

}
