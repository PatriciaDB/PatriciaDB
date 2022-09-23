package io.patriciadb.fs.disk;

public class OptimisticLockingFailure extends RuntimeException{
    public OptimisticLockingFailure() {
    }

    public OptimisticLockingFailure(String message) {
        super(message);
    }

    public OptimisticLockingFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
