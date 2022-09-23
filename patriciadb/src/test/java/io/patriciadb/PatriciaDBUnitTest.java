package io.patriciadb;

import io.patriciadb.fs.disk.DiskFileSystem;
import io.patriciadb.fs.simple.SimpleFileSystem;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class PatriciaDBUnitTest {

    @BeforeAll
    public static void addProvider() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void basicTest() throws Exception {
        var fs = new SimpleFileSystem();

        var patricDB = PatriciaDB.createNew(fs);

        var genesis = patricDB.startTransaction();
        try {
            var state = genesis.createOrOpenStorage("state".getBytes());
            state.put("hello".getBytes(), "world".getBytes());

            var philip = genesis.createOrOpenStorage("philip".getBytes());
            philip.put("hello".getBytes(), "philip".getBytes());

            genesis.commit("state1".getBytes());
        } finally {
            genesis.release();
        }

        var block = patricDB.getMetadataForBlockNumber(1);

        System.out.println(block);

        var block1 = patricDB.startTransaction("state1".getBytes());
        try {
            var state = block1.createOrOpenStorage("state".getBytes());
            System.out.println(new String(state.get("hello".getBytes())));

            var philip = block1.createOrOpenStorage("philip".getBytes());
            System.out.println(new String(philip.get("hello".getBytes())));

            block1.commit("state2".getBytes());
        } finally {
            block1.release();
        }
        fs.sync();
        fs.close();
    }


//    @Test
    public void benchmark() throws Exception {

        Path databaseDirectory = Files.createTempDirectory("patriciadb-benchmark");
        try {
            var fs = new DiskFileSystem(databaseDirectory, Integer.MAX_VALUE);

            var patricDB = PatriciaDB.createNew(fs);
            {
                var genesis = patricDB.startTransaction();
                try {
                    var state = genesis.createOrOpenStorage("state".getBytes());
                    state.put("hello".getBytes(), "world".getBytes());
                    genesis.commit("block0".getBytes());
                } finally {
                    genesis.release();
                }
            }

            long starttime = System.currentTimeMillis();
            Random r = new Random(1);
            byte[] randomKey = new byte[32];
            byte[] randomVal = new byte[32];
            for (int block = 0; block < 40; block++) {
                long transactionTime = System.currentTimeMillis();
                var transaction = patricDB.startTransaction(("block" + block).getBytes());
                try {
                    var state = transaction.createOrOpenStorage(("state").getBytes());

                    for (int i = 0; i < 100_000; i++) {
                        r.nextBytes(randomKey);
                        r.nextBytes(randomVal);
                        state.put(randomKey, randomVal);
                    }

                    transaction.commit(("block" + (block + 1)).getBytes());
                } finally {
                    transaction.release();
                }
                fs.sync();
                System.out.printf("Transaction %d time %d%n", block, System.currentTimeMillis() - transactionTime);
            }
            System.out.printf("Total Time %d%n", System.currentTimeMillis() - starttime);
        }finally {
            FileUtils.deleteDirectory(databaseDirectory.toFile());
        }
    }
}
