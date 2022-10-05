package io.patriciadb.fs.disk.transaction;

public interface TransactionHandler {
      void commit(TransactionSession handler);

      void release(TransactionSession handler);

}
