package io.patriciadb.benchmarks.besu;


import io.patriciadb.benchmarks.utils.KeyValueGenerator;
import io.patriciadb.benchmarks.utils.LengthRange;
import io.patriciadb.utils.Space;
import org.apache.commons.io.FileUtils;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.ethereum.trie.KeyValueMerkleStorage;
import org.hyperledger.besu.ethereum.trie.StoredMerklePatriciaTrie;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Files;
import java.util.Random;
import java.util.function.Function;

public class BesuPmtBenchmark {

    public static void main(String[] args) throws Exception {

        var tempDir = Files.createTempDirectory("besu-benchmark");

        try {
            System.out.println("Temp directory " + tempDir);
            System.out.println("It will be clean at the end of the test");
            System.out.println("Inserting 40 batch, 100K key/values each batch; Total of 4M KeyValues");
            var kvGenerator = new KeyValueGenerator(1, 10_000_000, LengthRange.ofSize(32), LengthRange.ofSize(32));
            var keyGeneratorIterator = kvGenerator.iterable().iterator();
            try (final Options options = new Options().setCreateIfMissing(true)) {

                try (final RocksDB db = RocksDB.open(options, tempDir.toString())) {
                    BenchmarkRocksStorage myRockKVStorage = new BenchmarkRocksStorage(db);
                    var merkleStorage = new KeyValueMerkleStorage(myRockKVStorage);

                    StoredMerklePatriciaTrie<Bytes, Bytes> s = new StoredMerklePatriciaTrie<>(merkleStorage::get, Function.identity(), Function.identity());

                    var rootHash = s.getRootHash();
                    s.commit(merkleStorage::put);


                    long startTime = System.currentTimeMillis();
                    for (int batch = 0; batch < 40; batch++) {
                        long batchStart = System.currentTimeMillis();
                        StoredMerklePatriciaTrie<Bytes, Bytes> storage = new StoredMerklePatriciaTrie<>(merkleStorage::get, rootHash, Function.identity(), Function.identity());
                        for (int i = 0; i < 100_000; i++) {
                            var kv = keyGeneratorIterator.next();
                            storage.put(Bytes.wrap(kv.key()), Bytes.wrap(kv.value()));
                        }
                        rootHash = storage.getRootHash();
                        storage.commit(merkleStorage::put);
                        merkleStorage.commit();
                        System.out.printf("Batch %d completed in %d%n", batch, (System.currentTimeMillis() - batchStart));
                    }
                    System.out.println("Find root Hash "+rootHash);
                    System.out.printf("Job completed in %d%n", System.currentTimeMillis() - startTime);
                    System.out.printf("Size of database before compaction %dMB %n", Space.MEGABYTE.fromBytes(FileUtils.sizeOfDirectory(tempDir.toFile())));
                    long compactTime = System.currentTimeMillis();
                    db.compactRange();
                    System.out.printf("Compaction time %d%n", System.currentTimeMillis() - compactTime);
                    System.out.printf("Size of database after compaction %dMB %n", Space.MEGABYTE.fromBytes(FileUtils.sizeOfDirectory(tempDir.toFile())));

                }
            } catch (RocksDBException e) {

            }
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}