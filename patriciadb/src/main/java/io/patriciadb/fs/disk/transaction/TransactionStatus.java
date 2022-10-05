package io.patriciadb.fs.disk.transaction;

public enum TransactionStatus {
    RUNNING,
    COMMITTED,
    // Status also used for released read-only transactions
    ROLLED_BACK,
    FAILURE
}
