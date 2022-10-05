package io.patriciadb.fs.disk;

import io.patriciadb.fs.PatriciaFileSystemFactory;
import io.patriciadb.fs.properties.FileSystemType;
import io.patriciadb.fs.properties.PropertyConstants;
import io.patriciadb.utils.Space;
import org.apache.commons.io.FileUtils;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DiskFileSystemWriteReadUnitTests {


    @Test
    public void closeOpenDiskStorage() throws Exception {
        var tempDir = Files.createTempDirectory("patricia-test");
        var props = new HashMap<String, String>();
        props.put(PropertyConstants.FS_TYPE, FileSystemType.APPENDER.toString());
        props.put(PropertyConstants.FS_DATA_FOLDER, tempDir.toString());
        props.put(PropertyConstants.FS_MAX_DATA_FILE_SIZE, String.valueOf(Space.MEGABYTE.toBytes(256)));
        try {
            System.out.println(tempDir);
            var fs = PatriciaFileSystemFactory.createFromProperties(props);
            int blockToWrite = 1_000_000;
            int totalBatch = 20;

            LongArrayList blockids = new LongArrayList();
            {
                int counter = 1;
                var payload = ByteBuffer.allocate(64);
                for (var batch = 0; batch < totalBatch; batch++) {
                    var transaction = fs.startTransaction();
                    for (int x = 0; x < blockToWrite; x++) {
                        payload.position(0);
                        payload.putLong(counter++);
                        payload.position(0);
                        payload.limit(64);
                        long blockId = transaction.write(payload);
                        blockids.add(blockId);
                    }
                    transaction.commit();
                    transaction.release();
                }
            }
            fs.close();

            var fs2 = PatriciaFileSystemFactory.createFromProperties(props);
            var tr2 = fs2.startTransaction();
            var expected = ByteBuffer.allocate(64);
            int counter = 1;
            for (int i = 0; i < blockids.size(); i++) {
                long blockId = blockids.get(i);

                expected.position(0);
                expected.putLong(counter++);
                expected.position(0);
                expected.limit(64);
                var buffer = tr2.read(blockId);
                byte[] payload = new byte[buffer.remaining()];
                buffer.get(payload);
                Assertions.assertArrayEquals(expected.array(), payload);
            }
            fs2.close();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }

    }

    @Test
    public void reuseUnusedBlockIds() throws Exception {
        var tempDir = Files.createTempDirectory("patricia-test");
        var props = new HashMap<String, String>();
        props.put(PropertyConstants.FS_TYPE, FileSystemType.APPENDER.toString());
        props.put(PropertyConstants.FS_DATA_FOLDER, tempDir.toString());
        props.put(PropertyConstants.FS_MAX_DATA_FILE_SIZE, String.valueOf(Space.MEGABYTE.toBytes(256)));
        try {
            System.out.println(tempDir);
            var fs = PatriciaFileSystemFactory.createFromProperties(props);
            var tr = fs.startTransaction();
            Set<Long> blockIds = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                long block = tr.write(ByteBuffer.allocate(10));
                blockIds.add(block);
            }
            tr.release();// Equivalent of rolling back

            var tr2 = fs.startTransaction();

            for (int i = 0; i < 100; i++) {
                long block = tr2.write(ByteBuffer.allocate(10));
                if (!blockIds.contains(block)) {
                    throw new IllegalStateException();
                }
            }
            tr2.release();


        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}
