package io.patriciadb.core.transactionstable;

import io.patriciadb.table.TableRead;

import java.util.List;
import java.util.Optional;

public interface TransactionTableRead extends TableRead<TransactionEntity> {

    Optional<TransactionEntity> findByBlockHash(byte[] blockHash);

    List<TransactionEntity> findByParentBlockHash(byte[] parentBlockHash);

    List<TransactionEntity> findByBlockNumber(long blockNumber);
}
