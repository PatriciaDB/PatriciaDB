package io.patriciadb.core.blocktable;

import io.patriciadb.table.TableRead;

import java.util.List;
import java.util.Optional;

public interface BlockTableRead extends TableRead<BlockEntity> {

    Optional<BlockEntity> findByBlockHash(byte[] blockHash);

    List<BlockEntity> findByParentBlockHash(byte[] parentBlockHash);

    List<BlockEntity> findByBlockNumber(long blockNumber);
}
