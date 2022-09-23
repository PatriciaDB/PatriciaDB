package io.patriciadb.fs.disk;

import io.patriciadb.fs.FileSystemError;

public class DirectoryError extends FileSystemError {

    public DirectoryError(boolean isFatal) {
        super(isFatal);
    }

    public DirectoryError(boolean isFatal, String message) {
        super(isFatal, message);
    }

    public DirectoryError(boolean isFatal, String message, Throwable cause) {
        super(isFatal, message, cause);
    }
    public DirectoryError(boolean isFatal, Throwable cause) {
        super(isFatal, cause);
    }
}
