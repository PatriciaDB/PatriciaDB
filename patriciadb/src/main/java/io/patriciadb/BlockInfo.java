package io.patriciadb;

import java.time.Instant;

public interface BlockInfo {
    byte[] blockHash();

    byte[] parentBlockHash();

    long blockNumber();

    Instant creationTime();

    String attachment();

}
