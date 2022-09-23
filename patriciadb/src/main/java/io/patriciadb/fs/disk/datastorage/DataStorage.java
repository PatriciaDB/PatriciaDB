package io.patriciadb.fs.disk.datastorage;

import io.patriciadb.fs.disk.StorageIoException;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface DataStorage extends Closeable {
    ByteBuffer read(long blockPointer) throws StorageIoException;

    void flush() throws StorageIoException;

    void flushAndSync() throws StorageIoException ;

    long write(ByteBuffer payload) throws StorageIoException;
}
