package io.patriciadb.fs.disk;

import io.patriciadb.fs.FileSystemError;

public class StorageOpenException extends FileSystemError {

    public StorageOpenException() {
        super(true);
    }

    public StorageOpenException(String message) {
        super(true, message);
    }

    public StorageOpenException(String message, Throwable cause) {
        super(true, message, cause);
    }

    public StorageOpenException(Throwable cause) {
        super(true, cause);
    }
}
