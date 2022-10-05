package io.patriciadb.fs.disk.transaction;

import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

public interface TransactionSession {
    TransactionStatus getStatus();

    void setStatus(TransactionStatus newStatus);

    void addDeltaChange(LongLongHashMap deltaChange);

    long getId();
}
