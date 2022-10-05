package io.patriciadb.fs.disk.transaction;

import io.patriciadb.fs.FsReadTransaction;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.directory.Directory;

import java.nio.ByteBuffer;

public class TransactionReadSession extends TransactionSessionAbs implements TransactionSession{

    public TransactionReadSession(TransactionHandler transactionHandler,
                                  long transactionId,
                                  DataStorage dataStorage,
                                  Directory directory) {

        super(transactionHandler, transactionId, dataStorage, directory);
    }


    public FsReadTransaction createSession() {
        return new FsReadTransactionImp();
    }

    private synchronized long readPointer(long blockId) {
        for (var elem : prevChanges) {
            if (elem.containsKey(blockId)) {
                return elem.get(blockId);
            }
        }
        return directory.get(blockId);
    }

    private void checkState() {
        if(status.get()!=TransactionStatus.RUNNING) {
            throw new IllegalStateException("Transaction is disposed");
        }
    }


    private class FsReadTransactionImp implements FsReadTransaction {

        @Override
        public synchronized ByteBuffer read(long blockId) {
            checkState();
            long dataPointer = readPointer(blockId);
            if (dataPointer == 0) {
                return null;
            }
            return dataStorage.read(dataPointer);
        }

        @Override
        public synchronized void release() {
            transactionHandler.release(TransactionReadSession.this);
        }
    }
}
