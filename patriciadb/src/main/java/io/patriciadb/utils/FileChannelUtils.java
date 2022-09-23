package io.patriciadb.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelUtils {

    public static long writeFully(FileChannel ch, ByteBuffer buffer) throws IOException {
        long written = 0;
        while (buffer.hasRemaining()) {
            written += ch.write(buffer);
        }
        return written;
    }

    public static long writeFully(FileChannel ch, ByteBuffer buffer, long position) throws IOException {
        long written = 0;
        while (buffer.hasRemaining()) {
            written += ch.write(buffer, position + written);
        }
        return written;
    }

    public static void readFully(FileChannel ch, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = ch.read(buffer);
            if (read == -1) {
                throw new IOException("EndOfFile");
            }
        }
    }

    public static void readFully(FileChannel ch, ByteBuffer buffer, long position) throws IOException {
        int readTotal = 0;
        while (buffer.hasRemaining()) {
            int read = ch.read(buffer, position + readTotal);
            if (read == -1) {
                throw new IOException("EndOfFile");
            }
            readTotal += read;
        }
    }
}
