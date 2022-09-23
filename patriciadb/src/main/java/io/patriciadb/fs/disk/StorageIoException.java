package io.patriciadb.fs.disk;

import io.patriciadb.fs.FileSystemError;

public class StorageIoException extends FileSystemError {

    public StorageIoException() {
        super(true);
    }

    public StorageIoException(String message) {
        super(true, message);
    }

    public StorageIoException(String message, Throwable cause) {
        super(true,message, cause);
    }

    public StorageIoException(Throwable cause) {
        super(true,cause);
    }
}
