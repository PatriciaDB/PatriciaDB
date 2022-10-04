package io.patriciadb.benchmarks.patriciadb;

import io.patriciadb.PatriciaDB;
import io.patriciadb.benchmarks.utils.KeyValueGenerator;
import io.patriciadb.benchmarks.utils.LengthRange;
import io.patriciadb.fs.PatriciaFileSystemFactory;
import io.patriciadb.fs.properties.FileSystemType;
import io.patriciadb.fs.properties.PropertyConstants;
import io.patriciadb.utils.Space;
import org.apache.commons.io.FileUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.HashMap;
import java.util.HexFormat;

public class PatriciaDBBenchmark {

    public static void main(String[] args) throws Exception {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        Path tempDir = Files.createTempDirectory("patriciadb-benchmark");
        try {
            System.out.println("Temp directory " + tempDir);
            System.out.println("It will be clean at the end of the test");
            System.out.println("Inserting 40 batch, 100K key/values each batch; Total of 4M KeyValues");
            var kvGenerator = new KeyValueGenerator(1, 10_000_000, LengthRange.ofSize(32), LengthRange.ofSize(32));
            var keyGeneratorIterator = kvGenerator.iterable().iterator();
            var props = new HashMap<String, String>();
            props.put(PropertyConstants.FS_DATA_FOLDER, tempDir.toString());
            props.put(PropertyConstants.FS_TYPE, FileSystemType.APPENDER.toString());


            var patricDB = PatriciaDB.createNew(props);

            var tr = patricDB.startTransaction();
            try {
                tr.commit("block0".getBytes());
            } finally {
                tr.release();
            }

            long starttime = System.currentTimeMillis();
            byte[] rootHash = null;
            for (int block = 0; block < 40; block++) {
                long transactionTime = System.currentTimeMillis();
                var transaction = patricDB.startTransaction(("block" + block).getBytes());
                try {
                    var state = transaction.createOrOpenStorage(("state").getBytes());

                    for (int i = 0; i < 100_000; i++) {
                        var kv = keyGeneratorIterator.next();
                        state.put(kv.key(), kv.value());
                    }
                    rootHash = state.rootHash();
                    transaction.commit(("block" + (block + 1)).getBytes());
                } finally {
                    transaction.release();
                }
                System.out.printf("Transaction %d time %d%n", block, System.currentTimeMillis() - transactionTime);
            }
            System.out.printf("Final root hash %s%n", HexFormat.of().withDelimiter(":").withLowerCase().formatHex(rootHash));
            System.out.printf("Total Time %d%n", System.currentTimeMillis() - starttime);
            System.out.printf("Database size before vacuum %dMB%n", Space.MEGABYTE.fromBytes(FileUtils.sizeOfDirectory(tempDir.toFile())));
            patricDB.close();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}
