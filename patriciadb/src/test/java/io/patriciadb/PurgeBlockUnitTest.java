package io.patriciadb;

import io.patriciadb.core.BlockInfoRecord;
import io.patriciadb.fs.simple.SimpleFileSystem;
import org.apache.logging.log4j.core.util.Assert;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.Security;

public class PurgeBlockUnitTest {

    @BeforeAll
    public static void addProvider() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void basicPurgeTest() {
        var inMemory = new SimpleFileSystem();

        var db =PatriciaDB.createNew(inMemory);

        var tr = db.startTransaction();
        var storage = tr.createOrOpenStorage("storage1".getBytes());
        storage.put("hello".getBytes(), "world".getBytes());
        tr.commit("block0".getBytes());
        tr.release();
        var blockInfo =db.getMetadata("block0".getBytes()).map(BlockInfoRecord.class::cast).get();
        System.out.println(blockInfo);
        System.out.println(blockInfo.newNodeIds());
        System.out.println(blockInfo.lostNodeIds());


         tr = db.startTransaction("block0".getBytes());
         storage = tr.createOrOpenStorage("storage1".getBytes());
        storage.put("hello".getBytes(), "philip".getBytes());
        storage.put("hell".getBytes(), "no!".getBytes());
        storage.put("dog".getBytes(), "animal".getBytes());
        tr.commit("block1".getBytes());
        tr.release();
        blockInfo =db.getMetadata("block1".getBytes()).map(BlockInfoRecord.class::cast).get();
        System.out.println(blockInfo);
        System.out.println(blockInfo.newNodeIds());
        System.out.println(blockInfo.lostNodeIds());



        tr = db.startTransaction("block1".getBytes());
        storage = tr.createOrOpenStorage("storage1".getBytes());
        storage.put("dog".getBytes(), "pug".getBytes());
        tr.commit("block2".getBytes());
        tr.release();
        blockInfo =db.getMetadata("block2".getBytes()).map(BlockInfoRecord.class::cast).get();
        System.out.println(blockInfo);
        System.out.println(blockInfo.newNodeIds());
        System.out.println(blockInfo.lostNodeIds());

        System.out.println("Current state");
        for(int block=0; block<5; block++) {
            var opt =db.getMetadata(("block"+block).getBytes()).map(BlockInfoRecord.class::cast);
            if(opt.isPresent()) {
                System.out.printf("Block %d= %s%n", block, opt.get());
                System.out.println(opt.get().newNodeIds());
                System.out.println(opt.get().lostNodeIds());
            }
        }

        System.out.println("Purging block1");

        db.purgeBlockData("block1".getBytes());

        System.out.println("New state");
        for(int block=0; block<5; block++) {
            var opt =db.getMetadata(("block"+block).getBytes()).map(BlockInfoRecord.class::cast);
            if(opt.isPresent()) {
                System.out.printf("Block %d= %s%n", block, opt.get());
                System.out.println(opt.get().newNodeIds());
                System.out.println(opt.get().lostNodeIds());
            }
        }


        tr = db.startTransaction("block0".getBytes());
        storage = tr.createOrOpenStorage("storage1".getBytes());
        Assertions.assertEquals("world", new String(storage.get("hello".getBytes())));
        tr.release();

        tr = db.startTransaction("block2".getBytes());
        storage = tr.createOrOpenStorage("storage1".getBytes());
        Assertions.assertEquals("philip", new String(storage.get("hello".getBytes())));
        tr.release();
    }
}
