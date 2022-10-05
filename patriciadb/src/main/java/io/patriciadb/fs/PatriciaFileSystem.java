package io.patriciadb.fs;

import io.patriciadb.utils.ExceptionUtils;

import java.util.function.Function;

public interface PatriciaFileSystem {

    FsReadTransaction getSnapshot();

    FsWriteTransaction startTransaction();

    void runVacuum();


    default <T> T startTransaction(Function<FsWriteTransaction, T> runnable) {
        var tr = startTransaction();
        try {
            var res= runnable.apply(tr);
            tr.commit();;
            return res;
        }catch (Throwable t) {
            throw ExceptionUtils.sneakyThrow(t);
        } finally {
            tr.release();
        }
    }

    void close() throws FileSystemError;


    default <T> T getSnapshot(Function<FsReadTransaction, T> runnable) {
        var tr = getSnapshot();
        try {
            return runnable.apply(tr);
        }catch (Throwable t) {
            throw ExceptionUtils.sneakyThrow(t);
        } finally {
            tr.release();
        }
    }
}
