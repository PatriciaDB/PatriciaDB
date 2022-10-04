package io.patriciadb;

import io.patriciadb.fs.properties.FileSystemType;
import io.patriciadb.fs.properties.PropertyConstants;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PatriciaDBUnitTest {

    @BeforeAll
    public static void addProvider() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void basicTest() throws Exception {

        var patricDB = PatriciaDB.createNew(Map.of());

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
    }

}
