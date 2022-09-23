package io.patriciadb.fs.disk.directory.wal;

import io.patriciadb.utils.FileChannelUtils;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32;

public class DirectoryLogFileReader implements Closeable {
    private final FileChannel ch;

    public DirectoryLogFileReader(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("File does not exist");
        }
        ch = FileChannel.open(path, StandardOpenOption.READ);
        ch.position(0);

    }

    public Optional<LongLongHashMap> readNextBlock() throws IOException {
        return readNextSegment(ch);
    }

    public void close() throws IOException{
        ch.close();
    }


    private static Optional<LongLongHashMap> readNextSegment(FileChannel ch) throws IOException {
        if(ch.position()==ch.size()) {
            return Optional.empty();
        }
        CRC32 crc32 = new CRC32();


        ByteBuffer lengthBuffer = ByteBuffer.allocate(Long.BYTES);
        FileChannelUtils.readFully(ch, lengthBuffer);
        lengthBuffer.flip();
        int size = Math.toIntExact(lengthBuffer.getLong());
        crc32.update(lengthBuffer.array());


        var entryBuffer = ByteBuffer.allocate(size * Long.BYTES * 2);
        FileChannelUtils.readFully(ch, entryBuffer);
        entryBuffer.flip();
        crc32.update(entryBuffer.array());

        LongLongHashMap result = new LongLongHashMap();
        for (int i = 0; i < size; i++) {
            long blockId = entryBuffer.getLong();
            long pointer = entryBuffer.getLong();
            result.put(blockId, pointer);
        }

        var crcBuffer = ByteBuffer.allocate(Long.BYTES);
        FileChannelUtils.readFully(ch, crcBuffer);
        crcBuffer.flip();
        long computedCrc = crc32.getValue();
        long savedCrc =crcBuffer.getLong();

        if(computedCrc != savedCrc) {
            throw new IOException("Block CRC invalid, transaction will not be persisted");
        }
        return Optional.of(result);
    }
}
