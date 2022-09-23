package io.patriciadb;

public class StorageNotFoundException extends RuntimeException {
    public StorageNotFoundException() {
    }

    public StorageNotFoundException(String message) {
        super(message);
    }

    public StorageNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
