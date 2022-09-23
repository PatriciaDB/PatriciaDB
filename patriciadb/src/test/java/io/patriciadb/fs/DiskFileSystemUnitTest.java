package io.patriciadb.fs;

import io.patriciadb.fs.disk.DiskFileSystem;
import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DiskFileSystemUnitTest {

    @Test
    public void basicTest() throws IOException {
        var tempDir = Files.createTempDirectory("patricia-test");
        try {
            System.out.println(tempDir);
            var fs = new DiskFileSystem(tempDir, 60 * 1024 * 1024);

            Random r;
            r = new Random(1);

            List<Long> blockids = new ArrayList<>();
            {
                byte[] payload = new byte[64];
                var transaction = fs.startTransaction();
                for (int x = 0; x < 100000; x++) {
                    r.nextBytes(payload);
                    long blockId = transaction.write(ByteBuffer.wrap(payload));
                    blockids.add(blockId);
                }
                transaction.commit();
                transaction.release();
            }

            r = new Random(1);
            var tr2 = fs.startTransaction();
            byte[] expected = new byte[64];

            for (var blockId : blockids) {
                r.nextBytes(expected);
                var buffer = tr2.read(blockId);
                byte[] payload = new byte[buffer.remaining()];
                buffer.get(payload);
                Assertions.assertArrayEquals(expected, payload);
            }
            fs.close();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }

    }
}
