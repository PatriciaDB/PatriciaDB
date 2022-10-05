package io.patriciadb.fs.disk.transaction;

public interface TransactionHandler {
      void commit(TransactionWriteSession handler);

      void release(TransactionSession handler);

}
