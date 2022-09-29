package io.patriciadb.fs.disk.directory.imp;



import io.patriciadb.utils.FileChannelUtils;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

public class LogFileWriter implements Closeable {

    private final FileChannel ch;
    private int cursor = 0;

    public LogFileWriter(Path path) throws IOException {
        ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }


    public  void appendAndSync(LongLongHashMap batch) throws IOException {
        // Format
        // size: long | Entry<Long,Long> | CRC32:long
        var buffer = ByteBuffer.allocate(batch.size() * Long.BYTES * 2 + Long.BYTES + Long.BYTES);//
        buffer.putLong(batch.size());
        for (var e : batch.keyValuesView()) {
            buffer.putLong(e.getOne());
            buffer.putLong(e.getTwo());
        }
        var crc = new CRC32();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putLong(crc.getValue());
        buffer.flip();
        cursor += FileChannelUtils.writeFully(ch, buffer, cursor);
        ch.force(false);
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    public  void reset() throws IOException {
        ch.truncate(0);
        cursor = 0;
    }

    public  long getLogSize() {
        return cursor;
    }

}
