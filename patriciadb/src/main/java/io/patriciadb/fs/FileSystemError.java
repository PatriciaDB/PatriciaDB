package io.patriciadb.fs;

public class FileSystemError extends RuntimeException {
    private final boolean isFatal;

    public FileSystemError(boolean isFatal) {
        this.isFatal = isFatal;
    }

    public FileSystemError(boolean isFatal, String message) {
        super(message);
        this.isFatal = isFatal;
    }

    public FileSystemError(boolean isFatal, String message, Throwable cause) {
        super(message, cause);
        this.isFatal = isFatal;
    }

    public FileSystemError(boolean isFatal, Throwable cause) {
        super(cause);
        this.isFatal = isFatal;
    }
}
