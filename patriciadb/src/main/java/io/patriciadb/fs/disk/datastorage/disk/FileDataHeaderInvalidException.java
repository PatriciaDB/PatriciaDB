package io.patriciadb.fs.disk.datastorage.disk;

public class FileDataHeaderInvalidException extends Exception {
    public FileDataHeaderInvalidException() {
    }

    public FileDataHeaderInvalidException(String message) {
        super(message);
    }

    public FileDataHeaderInvalidException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileDataHeaderInvalidException(Throwable cause) {
        super(cause);
    }
}
