package io.patriciadb.table;

public class UniqueConstrainViolation extends RuntimeException {

    public UniqueConstrainViolation() {
    }

    public UniqueConstrainViolation(String message) {
        super(message);
    }

    public UniqueConstrainViolation(String message, Throwable cause) {
        super(message, cause);
    }
}
