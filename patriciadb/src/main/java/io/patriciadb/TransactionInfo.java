package io.patriciadb;

import java.time.Instant;

public interface TransactionInfo {
    byte[] transactionId();

    byte[] parentTransactionId();

    long blockNumber();

    Instant creationTime();

    byte[] attachment();

}
